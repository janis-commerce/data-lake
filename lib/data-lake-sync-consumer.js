'use strict';

const crypto = require('crypto');
const { createGzip } = require('zlib');
const { Transform, PassThrough } = require('stream');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const { IterativeSQSConsumer } = require('@janiscommerce/sqs-consumer');
const { struct } = require('@janiscommerce/superstruct');

const INITIAL_LOAD_MAX_MB = 500;

const s3 = new S3Client();

const ModelFetcher = require('./helpers/model-fetcher');
const getEntitySettings = require('./helpers/get-entity-settings');

const mainStruct = struct.partial({
	entity: 'string',
	incremental: 'boolean',
	from: 'string',
	to: 'string',
	limit: 'number?',
	maxSizeMB: 'number?'
});

module.exports = class DataLakeSyncConsumer extends IterativeSQSConsumer {

	async processSingleRecord(record, logger) {

		const [error] = mainStruct.validate(record?.body);

		if(error) {
			logger.error(`[${this.session.clientCode}] Invalid record`, { record: JSON.stringify(record?.body), error });
			return;
		}

		const {
			entity, incremental, from, to, limit, maxSizeMB
		} = record.body;

		const dateField = incremental ? 'dateModified' : 'dateCreated';

		const model = ModelFetcher.get(entity);

		const entitySettings = getEntitySettings(entity);

		const modelInstance = this.session.getSessionInstance(model);
		const cursor = await modelInstance.get({
			...entitySettings.fields && { fields: entitySettings.fields },
			order: { dateCreated: 'asc' },
			filters: {
				[`${dateField}From`]: new Date(from),
				...to && { [`${dateField}To`]: new Date(to) }
			},
			returnType: 'cursor'
		});

		const batchSize = limit || 1000;

		cursor.batchSize(batchSize);

		const cursorStream = cursor.stream();

		const pushedAt = Date.now();

		const { clientCode } = this.session;

		const ndjsonTransform = new Transform({
			writableObjectMode: true,
			transform(entityData, encoding, callback) {

				entityData.id = entityData._id.toString(); // eslint-disable-line no-underscore-dangle
				delete entityData._id; // eslint-disable-line no-underscore-dangle

				const parsedObject = {
					uid: crypto.randomUUID(),
					clientCode,
					data: entityData,
					pushedAt
				};
				callback(null, JSON.stringify(parsedObject) + '\n');
			}
		});

		logger.info(`[${clientCode} - ${entity}] Starting to dump`);

		const maxSizeBytes = (maxSizeMB || INITIAL_LOAD_MAX_MB) * 1024 * 1024;

		const now = new Date();
		const keyPrefix = [
			`microservice=${process.env.JANIS_SERVICE_NAME}`,
			`entity=${entity}`,
			`load_type=${incremental ? 'incremental' : 'initial'}`,
			`client_code=${clientCode}`,
			`year=${now.getFullYear()}`,
			`month=${String(now.getMonth() + 1).padStart(2, '0')}`,
			`day=${String(now.getDate()).padStart(2, '0')}`
		].join('/');

		let partIndex = 0;
		let currentBytes = 0;
		let currentPassThrough;
		let ndjsonStream;
		const uploadPromises = [];

		const startNewPart = () => {
			partIndex += 1;
			currentBytes = 0;
			currentPassThrough = new PassThrough();
			const gzip = createGzip({ level: 6 });
			const key = `${keyPrefix}/${from}-${pushedAt}-${String(partIndex).padStart(3, '0')}.ndjson.gz`;

			logger.info(`[${clientCode} - ${entity}] Starting new S3 upload part ${partIndex} → ${key}`);

			const uploader = new Upload({
				client: s3,
				params: {
					Bucket: process.env.S3_DATA_LAKE_RAW_BUCKET,
					Key: key,
					Body: currentPassThrough.pipe(gzip),
					ContentType: 'application/gzip'
				}
			});

			uploader.on('httpUploadProgress', progress => {
				if(progress && progress.total)
					logger.debug(`[${clientCode} - ${entity}] Upload part ${partIndex} progress: ${progress.loaded}/${progress.total}`);
			});

			uploadPromises.push(uploader.done());
		};

		const shouldRotate = lineSize => {
			return !currentPassThrough
				|| (currentBytes + lineSize) > maxSizeBytes;
		};

		await new Promise((resolve, reject) => {
			ndjsonStream = cursorStream
				.pipe(ndjsonTransform)
				.on('error', reject);

			ndjsonStream.on('data', line => {
				try {
					const lineSize = Buffer.byteLength(line, 'utf8');
					if(shouldRotate(lineSize)) {
						if(currentPassThrough)
							currentPassThrough.end();
						startNewPart();
					}

					currentBytes += lineSize;

					const canContinue = currentPassThrough.write(line);
					if(!canContinue) {
						ndjsonStream.pause();
						currentPassThrough.once('drain', () => ndjsonStream.resume());
					}
				} catch(err) {
					reject(err);
				}
			});

			ndjsonStream.on('end', () => {
				try {
					if(currentPassThrough)
						currentPassThrough.end();
					resolve();
				} catch(err) {
					reject(err);
				}
			});
		});

		await Promise.all(uploadPromises);

		logger.info(`[${clientCode} - ${entity}] Finished dumping and uploaded to S3`);
	}
};
