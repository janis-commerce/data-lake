'use strict';

const { LambdaWithPayload } = require('@janiscommerce/lambda');
const { ApiSession } = require('@janiscommerce/api-session');
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');
const { struct } = require('@janiscommerce/superstruct');
const logger = require('lllog')();

const ModelFetcher = require('./helpers/model-fetcher');
const getEntitySettings = require('./helpers/get-entity-settings');
const kebabCase = require('./helpers/kebab-case');

const mainStruct = struct.partial({
	entity: 'string',
	incremental: 'boolean?',
	from: 'string?',
	to: 'string?',
	limit: 'number?',
	maxSizeMB: 'number?',
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

		const method = this.data.incremental ? 'sendIncrementalLoadMessages' : 'sendInitialLoadMessages';
		const now = new Date();

		for(const { code, settings } of clients)
			await this[method](code, now, settings);
	}

	async getClients() {

		const ClientModel = ModelFetcher.get('client');

		this.clientModel = new ClientModel();

		const clients = await this.clientModel.get({
			fields: [
				'code',
				`settings.${this.entity}.lastIncrementalLoadDate`
			],
			filters: {
				...this.data.clientCode && { code: this.data.clientCode },
				status: ClientModel.statuses.active
			}
		});

		return clients;
	}

	async sendIncrementalLoadMessages(clientCode, now, settings) {

		const lastIncrementalLoadDate = settings?.[this.entity]?.lastIncrementalLoadDate || this.entitySettings?.initialLoadDate;

		if(!lastIncrementalLoadDate)
			throw new Error(`Missing initialLoadDate for entity "${this.entity}"`);

		const from = new Date(lastIncrementalLoadDate);

		const limitDate = new Date(lastIncrementalLoadDate);
		limitDate.setDate(limitDate.getDate() + 1);

		const to = limitDate.getTime() > now.getTime() ? now : limitDate;

		const response = await this.sendMessages(clientCode, [{
			content: {
				entity: this.entity,
				incremental: true,
				from,
				to
			}
		}]);

		if(response) {
			return this.clientModel.update({
				[`settings.${this.entity}.lastIncrementalLoadDate`]: to
			}, {
				code: clientCode
			});
		}
	}

	async sendInitialLoadMessages(clientCode) {

		if(!this.data.from)
			throw new Error('From date is required for initial load');

		logger.info(`[${clientCode} - ${this.entity}] Starting Initial Load`);

		const fromDate = new Date(this.data.from);
		const toDate = this.data.to ? new Date(this.data.to) : new Date();

		fromDate.setHours(0, 0, 0, 0);
		toDate.setHours(23, 59, 59, 999);

		let messages = [];
		const currentDate = new Date(fromDate);

		while(currentDate <= toDate) {
			const dayStart = new Date(currentDate);
			dayStart.setHours(0, 0, 0, 0);

			const dayEnd = new Date(currentDate);
			dayEnd.setHours(23, 59, 59, 999);

			const message = {
				content: {
					entity: this.entity,
					from: dayStart.toISOString(),
					to: dayEnd.toISOString(),
					incremental: false,
					...this.data.limit && { limit: this.data.limit },
					...this.data.maxSizeMB && { maxSizeMB: this.data.maxSizeMB }
				}
			};

			messages.push(message);

			if(messages.length === LIMIT_BATCH_MESSAGES) {
				await this.sendMessages(clientCode, messages);
				messages = [];
			}

			currentDate.setDate(currentDate.getDate() + 1);
		}

		if(messages.length > 0)
			await this.sendMessages(clientCode, messages);

		logger.info(`[${clientCode} - ${this.entity}] Initial Load completed`);
	}

	async sendMessages(clientCode, messages) {

		const session = new ApiSession({ clientCode });

		const sqsEmitter = session.getSessionInstance(SqsEmitter);

		const queueResponse = await sqsEmitter.publishEvents(process.env.DATA_LAKE_SYNC_SQS_QUEUE_URL, messages);

		if(queueResponse.failedCount) {

			const { incremental, from } = messages[0].content;
			const { to } = messages[messages.length - 1].content; // for initial load is the same message cause is just one message

			logger.error(
				`[${clientCode} - ${this.entity}] Failed to trigger Sync - ${incremental ? 'Incremental' : 'Initial Load'} - From ${from} To ${to}`,
				JSON.stringify(queueResponse.results)
			);

			return false;
		}

		return true;
	}
};
