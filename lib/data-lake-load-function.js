/* eslint-disable max-len */

'use strict';

const { LambdaWithPayload } = require('@janiscommerce/lambda');
const { ApiSession } = require('@janiscommerce/api-session');
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');
const { struct } = require('@janiscommerce/superstruct');
const logger = require('lllog')();

const ModelFetcher = require('./helpers/model-fetcher');
const getEntitySettings = require('./helpers/get-entity-settings');
const kebabCase = require('./helpers/kebab-case');
const arrayChunk = require('./helpers/array-chunk');

const mainStruct = struct.partial({
	entity: 'string',
	mode: 'string?',
	from: 'string?',
	to: 'string?',
	clientCode: 'string?'
});

const LIMIT_BATCH_MESSAGES = 50;

module.exports = class DataLakeLoadFunction extends LambdaWithPayload {

	get struct() {
		return mainStruct;
	}

	get entity() {
		this._entity ??= kebabCase(this.data.entity);
		return this._entity;
	}

	get entitySettings() {
		this._entitySettings ??= getEntitySettings(this.entity);
		return this._entitySettings;
	}

	async process() {

		const clients = await this.getClients();

		const method = this.data.mode === 'incremental' ? 'sendIncrementalLoadMessages' : 'sendInitialLoadMessages';
		const now = new Date();

		for(const { code, settings } of clients)
			await this[method](code, now, settings?.[this.entity]);
	}

	async getClients() {

		const ClientModel = ModelFetcher.get('client');

		this.clientModel = new ClientModel();

		const clients = await this.clientModel.get({
			fields: ['code', `settings.${this.entity}`],
			filters: {
				...this.data.clientCode && { code: this.data.clientCode },
				status: ClientModel.statuses.active
			}
		});

		return clients;
	}

	async sendIncrementalLoadMessages(clientCode, now, clientEntitySettings) {

		const incremental = clientEntitySettings?.lastIncrementalLoadDate;
		const initialStart = clientEntitySettings?.initialLoad?.dateStart;

		let lastIncrementalLoadDate;
		if(incremental && initialStart)
			lastIncrementalLoadDate = new Date(incremental).getTime() >= new Date(initialStart).getTime() ? incremental : initialStart;
		else
			lastIncrementalLoadDate = incremental || initialStart;

		if(!lastIncrementalLoadDate)
			throw new Error(`[${clientCode}] Could not find date 'from' for entity "${this.entity}" in client settings (${this.entity}.lastIncrementalLoadDate or ${this.entity}.initialLoad.dateStart)`);

		const from = new Date(lastIncrementalLoadDate);

		const limitDate = new Date(lastIncrementalLoadDate);
		limitDate.setDate(limitDate.getDate() + 1);

		const to = limitDate.getTime() > now.getTime() ? now : limitDate;

		const messages = await this.prepareIncrementalMessages(clientCode, {
			entity: this.entity,
			mode: 'incremental',
			from,
			to
		});

		const response = await this.sendMessages(clientCode, messages);

		if(response) {
			return this.clientModel.update({
				[`settings.${this.entity}.lastIncrementalLoadDate`]: to
			}, {
				code: clientCode
			});
		}
	}

	prepareIncrementalMessages(clientCode, content) {
		return [{ content }];
	}

	async sendInitialLoadMessages(clientCode, now, clientEntitySettings) {

		const messages = this.prepareInitialLoadMessages(clientCode, clientEntitySettings);

		logger.info(`[${clientCode} - ${this.entity}] Starting Initial Load (${messages.length} chunks)`);

		await this.sendMessages(clientCode, messages);

		logger.info(`[${clientCode} - ${this.entity}] Initial Load queued (${messages.length} chunks)`);
	}

	prepareInitialLoadMessages(clientCode, clientEntitySettings) {

		if(this.entitySettings?.initialLoad?.byId) {
			return [{
				content: {
					entity: this.entity,
					mode: 'initialById',
					...clientEntitySettings?.initialLoad?.lastId && {
						// resume from lastId if it exists
						lastId: clientEntitySettings?.initialLoad?.lastId
					}
				}
			}];
		}

		if(this.initialLoadMessages)
			return this.initialLoadMessages;

		let dateFromCursor = this.data.from || this.entitySettings?.initialLoad?.dateFrom;

		if(!dateFromCursor)
			throw new Error(`[${clientCode} - ${this.entity}] Could not find date 'from' - Validate settings file or send the date 'from' in the payload`);

		dateFromCursor = new Date(dateFromCursor);
		const dateTo = this.data.to ? new Date(this.data.to) : new Date();

		if(dateFromCursor.getTime() >= dateTo.getTime())
			throw new Error(`[${clientCode} - ${this.entity}] From date is greater than or equal to 'to' date`);

		dateFromCursor.setHours(0, 0, 0, 0);
		dateTo.setHours(23, 59, 59, 999);

		logger.info(`[${clientCode} - ${this.entity}] Starting Initial Load from ${dateFromCursor.toISOString()} to ${dateTo.toISOString()}`);

		this.initialLoadMessages = [];

		while(dateFromCursor <= dateTo) {

			dateFromCursor.setHours(0, 0, 0, 0);
			const dateFromString = dateFromCursor.toISOString();

			dateFromCursor.setHours(23, 59, 59, 999);
			const dateToString = dateFromCursor.toISOString();

			this.initialLoadMessages.push({
				content: {
					entity: this.entity,
					mode: 'initial',
					from: dateFromString,
					to: dateToString
				}
			});

			dateFromCursor.setDate(dateFromCursor.getDate() + 1);
		}

		return this.initialLoadMessages;
	}

	async sendMessages(clientCode, messages) {

		const session = new ApiSession({ clientCode });

		if(!this.sqsEmitter)
			this.sqsEmitter = session.getSessionInstance(SqsEmitter);
		else
			this.sqsEmitter.session = session;

		let response = true;

		const chunks = arrayChunk(messages, LIMIT_BATCH_MESSAGES);

		for(const chunk of chunks) {

			const queueResponse = await this.sqsEmitter.publishEvents(process.env.DATA_LAKE_SYNC_SQS_QUEUE_URL, chunk);

			if(queueResponse.failedCount) {

				const { mode, from } = messages[0].content;
				const { to } = messages[messages.length - 1].content;

				const suffix = from && to ? ` - ${from} to ${to}` : '';

				logger.error(
					`[${clientCode} - ${this.entity}] Failed to trigger Sync - ${mode}${suffix}`,
					JSON.stringify(queueResponse.results)
				);

				response = false;
				break;
			}
		}

		return response;
	}
};
