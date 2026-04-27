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

	async sendIncrementalLoadMessages(clientCode, now, clientSettings) {

		const lastIncrementalLoadDate = clientSettings?.[this.entity]?.lastIncrementalLoadDate || clientSettings?.[this.entity]?.initialLoad?.dateStart;

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

	async sendInitialLoadMessages(clientCode) {

		this.prepareInitialLoadMessages();

		logger.info(`[${clientCode} - ${this.entity}] Starting Initial Load`);

		for(const messages of this.initialLoadMessagesChunks)
			await this.sendMessages(clientCode, messages);

		logger.info(`[${clientCode} - ${this.entity}] Initial Load queued (${this.initialLoadMessagesCount} messages)`);
	}

	prepareInitialLoadMessages() {

		if(this.initialLoadMessagesChunks)
			return;

		if(this.entitySettings?.initialLoad?.byId) {
			this.initialLoadMessagesCount = 1;
			this.initialLoadMessagesChunks = [[{
				content: {
					entity: this.entity,
					mode: 'initialById'
				}
			}]];

			return;
		}

		let dateFrom = this.data.from || this.entitySettings?.initialLoad?.dateFrom;

		if(!dateFrom)
			throw new Error(`[${this.entity}] Could not find date 'from' - Validate settings file or send the date 'from' in the payload`);

		dateFrom = new Date(dateFrom);
		const dateTo = this.data.to ? new Date(this.data.to) : new Date();

		if(dateFrom.getTime() >= dateTo.getTime())
			throw new Error(`[${this.entity}] From date is greater than or equal to 'to' date`);

		dateFrom.setHours(0, 0, 0, 0);
		dateTo.setHours(23, 59, 59, 999);

		logger.info(`[${this.entity}] Starting Initial Load from ${dateFrom.toISOString()} to ${dateTo.toISOString()}`);

		const messageDate = new Date(dateFrom);
		const messages = [];

		while(messageDate <= dateTo) {

			const dayStart = new Date(messageDate);
			dayStart.setHours(0, 0, 0, 0);

			const dayEnd = new Date(messageDate);
			dayEnd.setHours(23, 59, 59, 999);

			const message = {
				content: {
					entity: this.entity,
					mode: 'initial',
					from: dayStart.toISOString(),
					to: dayEnd.toISOString()
				}
			};

			messages.push(message);

			messageDate.setDate(messageDate.getDate() + 1);
		}

		this.initialLoadMessagesCount = messages.length;
		this.initialLoadMessagesChunks = arrayChunk(messages, LIMIT_BATCH_MESSAGES);
	}

	async sendMessages(clientCode, messages) {

		const session = new ApiSession({ clientCode });

		if(!this.sqsEmitter)
			this.sqsEmitter = session.getSessionInstance(SqsEmitter);
		else
			this.sqsEmitter.session = session;

		const queueResponse = await this.sqsEmitter.publishEvents(process.env.DATA_LAKE_SYNC_SQS_QUEUE_URL, messages);

		if(queueResponse.failedCount) {

			const { mode, from } = messages[0].content;
			const { to } = messages[messages.length - 1].content;

			logger.error(
				`[${clientCode} - ${this.entity}] Failed to trigger Sync - ${mode} - ${from} to ${to}`,
				JSON.stringify(queueResponse.results)
			);

			return false;
		}

		return true;
	}
};
