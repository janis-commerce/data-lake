/* eslint-disable max-len */

'use strict';

const crypto = require('crypto');
const { Transform } = require('stream');
const { S3Client } = require('@aws-sdk/client-s3');
const { IterativeSQSConsumer } = require('@janiscommerce/sqs-consumer');
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');
const { struct } = require('@janiscommerce/superstruct');

const INITIAL_LOAD_MAX_MB = 500;
const DEFAULT_BATCH_SIZE = 1000;
const INITIAL_LOAD_BY_ID_BATCH_SIZE = 10000;
const INITIAL_LOAD_BY_ID_EXECUTION_LIMIT = 100000;

const s3 = new S3Client();

const ModelFetcher = require('./helpers/model-fetcher');
const getEntitySettings = require('./helpers/get-entity-settings');
const NdjsonGzipS3Uploader = require('./helpers/ndjson-gzip-s3-uploader');

const mainStruct = struct.partial({
	entity: 'string',
	mode: 'string',
	from: 'string?',
	to: 'string?',
	additionalFilters: 'object?',
	filenamePrefix: 'string?',
	limit: 'number?',
	lastId: 'string?'
});

module.exports = class DataLakeSyncConsumer extends IterativeSQSConsumer {

	async processSingleRecord(record, logger) {

		const body = this.validateRecord(record, logger);

		if(!body)
			return;

		if(body.mode === 'initialById') {
			await this.processInitialById(body, logger);
			return;
		}

		const { cursor, context } = await this.getCursor(body);
		await this.dumpToS3(cursor, context, logger);
	}

	validateRecord(record, logger) {

		const [error] = mainStruct.validate(record?.body);

		if(error) {
			logger.error(`[${this.session.clientCode}] Invalid record`, { record: JSON.stringify(record?.body), error });
			return null;
		}

		const { body } = record;

		if(body.mode !== 'initialById' && (!body.from || !body.to)) {
			logger.error(`[${this.session.clientCode}] Invalid record: 'from' and 'to' are required`, { record: JSON.stringify(body) });
			return null;
		}

		return body;
	}

	async processInitialById(body, logger) {

		const { entity, lastId: bodyLastId, filenamePrefix } = body;

		const model = ModelFetcher.get(entity);
		const entitySettings = getEntitySettings(entity);
		const modelInstance = this.session.getSessionInstance(model);

		const { clientCode } = this.session;
		const initialLoadSettings = entitySettings.initialLoad || {};
		const batchSize = initialLoadSettings.batchSize ?? INITIAL_LOAD_BY_ID_BATCH_SIZE;
		const executionLimit = initialLoadSettings.executionLimit ?? INITIAL_LOAD_BY_ID_EXECUTION_LIMIT;
		const maxSizeBytes = (entitySettings.maxSizeMB || INITIAL_LOAD_MAX_MB) * 1024 * 1024;

		const isFirstExecution = !bodyLastId;
		const executionDate = new Date();

		const pushedAt = Date.now();
		const now = new Date();
		const keyPrefix = [
			`microservice=${process.env.JANIS_SERVICE_NAME}`,
			`entity=${entity}`,
			'load_type=initial',
			`client_code=${clientCode}`,
			`year=${now.getFullYear()}`,
			`month=${String(now.getMonth() + 1).padStart(2, '0')}`,
			`day=${String(now.getDate()).padStart(2, '0')}`
		].join('/');

		const filenamePart = `byId-${bodyLastId || 'start'}`;
		const logRange = bodyLastId ? `from lastId=${bodyLastId}` : 'from start';

		logger.info(`[${clientCode} - ${entity} - Initial Load] Starting to dump ${logRange}`);

		const uploader = new NdjsonGzipS3Uploader({
			s3Client: s3,
			bucket: process.env.S3_DATA_LAKE_RAW_BUCKET,
			keyPrefix,
			filenamePart,
			filenamePrefix,
			pushedAt,
			maxSizeBytes,
			logger,
			clientCode,
			entity
		});

		let processed = 0;
		let lastProcessedId = bodyLastId || null;
		let lastBatchSize = 0;

		do {

			const items = await modelInstance.get({
				...entitySettings.fields && { fields: entitySettings.fields },
				...entitySettings.excludeFields && { excludeFields: entitySettings.excludeFields },
				order: { id: 'asc' },
				...(lastProcessedId && { filters: { id: { type: 'greater', value: lastProcessedId } } }),
				limit: batchSize,
				readPreference: entitySettings.readPreference || 'secondary'
			});

			lastBatchSize = items?.length || 0;

			if(!lastBatchSize)
				break;

			for(const entityData of items) {

				const parsedObject = {
					uid: crypto.randomUUID(),
					clientCode,
					data: entityData,
					pushedAt
				};

				const line = JSON.stringify(parsedObject) + '\n';

				await uploader.writeLine(line);
			}

			processed += lastBatchSize;
			lastProcessedId = items[items.length - 1].id;

		} while(lastBatchSize === batchSize && processed < executionLimit);

		await uploader.finalize();

		logger.info(`[${clientCode} - ${entity} - Initial Load] Finished dumping and uploaded to S3. Total items: ${processed}`);

		const finished = lastBatchSize < batchSize;

		await this.updateInitialLoadSettings({
			entity,
			isFirstExecution,
			processed,
			lastProcessedId,
			finished,
			executionDate
		});

		if(!finished)
			await this.enqueueNextByIdMessage(body, lastProcessedId, logger);
	}

	async updateInitialLoadSettings({
		entity, isFirstExecution, processed, lastProcessedId, finished, executionDate
	}) {

		const ClientModel = ModelFetcher.get('client');
		const clientModel = this.session.getSessionInstance(ClientModel);

		const updateValues = {
			...(processed > 0 && { $inc: { [`settings.${entity}.initialLoad.totalItems`]: processed } }),
			...(lastProcessedId && { [`settings.${entity}.initialLoad.lastId`]: lastProcessedId }),
			...(isFirstExecution && { [`settings.${entity}.initialLoad.dateStart`]: executionDate }),
			...(finished && { [`settings.${entity}.initialLoad.dateEnd`]: executionDate })
		};

		await clientModel.update(updateValues, { code: this.session.clientCode });
	}

	async getCursor(body) {

		const {
			entity, mode, from, to, additionalFilters, limit, filenamePrefix
		} = body;

		const model = ModelFetcher.get(entity);
		const entitySettings = getEntitySettings(entity);
		const modelInstance = this.session.getSessionInstance(model);

		const maxSizeBytes = (entitySettings.maxSizeMB || INITIAL_LOAD_MAX_MB) * 1024 * 1024;

		const { clientCode } = this.session;
		const pushedAt = Date.now();

		const now = new Date();
		const keyPrefix = [
			`microservice=${process.env.JANIS_SERVICE_NAME}`,
			`entity=${entity}`,
			`load_type=${mode === 'incremental' ? 'incremental' : 'initial'}`,
			`client_code=${clientCode}`,
			`year=${now.getFullYear()}`,
			`month=${String(now.getMonth() + 1).padStart(2, '0')}`,
			`day=${String(now.getDate()).padStart(2, '0')}`
		].join('/');

		let getParams;
		let filenamePart;
		let logRange;

		if(mode === 'incremental') {

			filenamePart = from;
			logRange = `${from} to ${to}`;

			getParams = {
				...entitySettings.fields && { fields: entitySettings.fields },
				...entitySettings.excludeFields && { excludeFields: entitySettings.excludeFields },
				...entitySettings.hint && { hint: entitySettings.hint },
				order: { dateModified: 'asc' },
				filters: {
					dateModifiedFrom: new Date(from),
					...to && { dateModifiedTo: new Date(to) },
					...additionalFilters && additionalFilters
				},
				returnType: 'cursor',
				readPreference: entitySettings.readPreference || 'secondary'
			};

		} else {

			filenamePart = from;
			logRange = `${from} to ${to}`;

			getParams = {
				...entitySettings.fields && { fields: entitySettings.fields },
				...entitySettings.excludeFields && { excludeFields: entitySettings.excludeFields },
				order: { dateCreated: 'asc' },
				filters: {
					dateCreatedFrom: new Date(from),
					...to && { dateCreatedTo: new Date(to) },
					...additionalFilters && additionalFilters
				},
				returnType: 'cursor',
				readPreference: entitySettings.readPreference || 'secondary'
			};
		}

		const cursor = await modelInstance.get(getParams);

		cursor.batchSize(limit || DEFAULT_BATCH_SIZE);

		const context = {
			clientCode,
			entity,
			mode,
			maxSizeBytes,
			filenamePrefix,
			pushedAt,
			keyPrefix,
			filenamePart,
			logRange
		};

		return { cursor, context };
	}

	async dumpToS3(cursor, context, logger) {

		const {
			clientCode, entity, mode, maxSizeBytes, filenamePrefix, pushedAt, keyPrefix, filenamePart, logRange
		} = context;

		const cursorStream = cursor.stream();

		let totalItems = 0;

		const ndjsonTransform = new Transform({
			writableObjectMode: true,
			transform(entityData, encoding, callback) {

				entityData.id = entityData._id.toString(); // eslint-disable-line no-underscore-dangle
				delete entityData._id; // eslint-disable-line no-underscore-dangle

				totalItems++;

				const parsedObject = {
					uid: crypto.randomUUID(),
					clientCode,
					data: entityData,
					pushedAt
				};
				callback(null, JSON.stringify(parsedObject) + '\n');
			}
		});

		logger.info(`[${clientCode} - ${entity} - ${mode}] Starting to dump ${logRange}`);

		const ndjsonUploader = new NdjsonGzipS3Uploader({
			s3Client: s3,
			bucket: process.env.S3_DATA_LAKE_RAW_BUCKET,
			keyPrefix,
			filenamePart,
			filenamePrefix,
			pushedAt,
			maxSizeBytes,
			logger,
			clientCode,
			entity
		});

		let ndjsonStream;

		await new Promise((resolve, reject) => {
			ndjsonStream = cursorStream
				.pipe(ndjsonTransform)
				.on('error', reject);

			ndjsonStream.on('data', line => {
				try {
					const canContinue = ndjsonUploader.writeLineWithBackpressure(line);
					if(!canContinue) {
						ndjsonStream.pause();
						ndjsonUploader.onNextDrain(() => ndjsonStream.resume());
					}
				} catch(err) {
					reject(err);
				}
			});

			ndjsonStream.on('end', () => {
				ndjsonUploader.finalize()
					.then(resolve)
					.catch(reject);
			});
		});

		logger.info(`[${clientCode} - ${entity} - ${mode}] Finished dumping and uploaded to S3. Total items: ${totalItems}`);
	}

	async enqueueNextByIdMessage(body, lastId, logger) {

		const { entity } = body;

		const sqsEmitter = this.session.getSessionInstance(SqsEmitter);

		const response = await sqsEmitter.publishEvents(process.env.DATA_LAKE_SYNC_SQS_QUEUE_URL, [{
			content: {
				entity,
				mode: 'initialById',
				lastId
			}
		}]);

		if(response?.failedCount) {
			logger.error(`[${this.session.clientCode} - ${entity}] Failed to re-queue initial load by id from lastId=${lastId}`);
			return;
		}

		logger.info(`[${this.session.clientCode} - ${entity}] Re-queued initial load by id from lastId=${lastId}`);
	}
};
