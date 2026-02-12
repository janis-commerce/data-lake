'use strict';

const assert = require('assert');
const sinon = require('sinon');

require('lllog')('none');

const { SqsEmitter } = require('@janiscommerce/sqs-emitter');
const Settings = require('@janiscommerce/settings');
const { Handler } = require('@janiscommerce/lambda');
const Model = require('@janiscommerce/model');

const DataLakeLoadFunction = require('../lib/data-lake-load-function');

const ModelFetcher = require('../lib/helpers/model-fetcher');

const QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/123/data-lake-sync';

describe('DataLakeLoadFunction', () => {

	const fakeCurrentDate = new Date('2026-02-01 00:00:00');

	const originalEnv = { ...process.env };

	const ClientModel = class ClientModel extends Model {};

	let settingsGetStub;
	let publishEventsStub;

	beforeEach(() => {

		sinon.useFakeTimers({ now: fakeCurrentDate });

		process.env.DATA_LAKE_SYNC_SQS_QUEUE_URL = QUEUE_URL;

		publishEventsStub = sinon.stub(SqsEmitter.prototype, 'publishEvents');
		publishEventsStub.resolves({ failedCount: 0 });

		sinon.stub(ModelFetcher, 'get').returns(ClientModel);

		sinon.stub(ClientModel.prototype, 'update').resolves(1);

		settingsGetStub = sinon.stub(Settings, 'get');
	});

	afterEach(() => {
		sinon.restore();
		process.env = { ...originalEnv };
	});

	const DataLakeLoad = body => Handler.handle(DataLakeLoadFunction, { body });

	describe('Incremental load', () => {

		const initialLoadDate = '2025-01-01 00:00:00';

		beforeEach(() => {
			settingsGetStub
				.returns({ entities: [{ name: 'order', initialLoadDate }] });
		});

		it('Should send message and update client when client has no lastIncrementalLoadDate for entity', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await DataLakeLoad({ entity: 'order', incremental: true });

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order.lastIncrementalLoadDate'],
				filters: { status: 'active' }
			});

			const from = new Date(initialLoadDate);
			const to = new Date(initialLoadDate);
			to.setDate(to.getDate() + 1);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					incremental: true,
					from,
					to
				}
			}]);

			sinon.assert.calledOnceWithExactly(ClientModel.prototype.update, {
				'settings.order.lastIncrementalLoadDate': to
			}, {
				code: 'client1'
			});
		});

		it('Should send message and update client when client has lastIncrementalLoadDate for entity', async () => {

			const lastIncrementalLoadDate = '2025-12-01 00:00:00';

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1', settings: { order: { lastIncrementalLoadDate } } }]);

			await DataLakeLoad({ entity: 'order', incremental: true });

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order.lastIncrementalLoadDate'],
				filters: { status: 'active' }
			});

			const from = new Date(lastIncrementalLoadDate);
			const to = new Date(lastIncrementalLoadDate);
			to.setDate(to.getDate() + 1);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					incremental: true,
					from,
					to
				}
			}]);

			sinon.assert.calledOnceWithExactly(ClientModel.prototype.update, {
				'settings.order.lastIncrementalLoadDate': to
			}, {
				code: 'client1'
			});
		});

		it('Should send message and update client when client has lastIncrementalLoadDate for entity and is today', async () => {

			const lastIncrementalLoadDate = new Date();
			lastIncrementalLoadDate.setHours(lastIncrementalLoadDate.getHours() - 3);

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1', settings: { order: { lastIncrementalLoadDate } } }]);

			await DataLakeLoad({ entity: 'order', incremental: true });

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order.lastIncrementalLoadDate'],
				filters: { status: 'active' }
			});

			const from = new Date(lastIncrementalLoadDate);
			const to = new Date(); // limited to now

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					incremental: true,
					from,
					to
				}
			}]);

			sinon.assert.calledOnceWithExactly(ClientModel.prototype.update, {
				'settings.order.lastIncrementalLoadDate': to
			}, {
				code: 'client1'
			});
		});

		it('Should not update client when publishEvents fails', async () => {

			publishEventsStub.resolves({ failedCount: 1 });

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await DataLakeLoad({ entity: 'order', incremental: true });

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order.lastIncrementalLoadDate'],
				filters: { status: 'active' }
			});

			const from = new Date(initialLoadDate);
			const to = new Date(initialLoadDate);
			to.setDate(to.getDate() + 1);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					incremental: true,
					from,
					to
				}
			}]);

			sinon.assert.notCalled(ClientModel.prototype.update);
		});

		it('Should throw when initialLoadDate is missing', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			settingsGetStub
				.returns({ entities: [{ name: 'order' }] });

			await assert.rejects(
				() => DataLakeLoad({ entity: 'order', incremental: true }),
				/Missing initialLoadDate for entity "order"/
			);
		});
	});

	describe('Initial load', () => {

		beforeEach(() => {
			settingsGetStub
				.returns({ entities: [{ name: 'order', initialLoadDate: '2025-01-01 00:00:00' }] });
		});

		afterEach(() => {
			sinon.assert.notCalled(ClientModel.prototype.update);
		});

		it('Should throw when from is missing', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await assert.rejects(
				() => DataLakeLoad({ entity: 'order', incremental: false }),
				/From date is required for initial load/
			);

			sinon.assert.notCalled(SqsEmitter.prototype.publishEvents);
		});

		it('Should send one message per day with correct from/to and incremental false', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await DataLakeLoad({
				entity: 'order',
				incremental: false,
				from: '2026-01-01 00:00:00',
				to: '2026-01-02 23:59:59'
			});

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order.lastIncrementalLoadDate'],
				filters: { status: 'active' }
			});

			const day1Start = new Date(2026, 0, 1, 0, 0, 0, 0);
			const day1End = new Date(2026, 0, 1, 23, 59, 59, 999);
			const day2Start = new Date(2026, 0, 2, 0, 0, 0, 0);
			const day2End = new Date(2026, 0, 2, 23, 59, 59, 999);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					from: day1Start.toISOString(),
					to: day1End.toISOString(),
					incremental: false
				}
			}, {
				content: {
					entity: 'order',
					from: day2Start.toISOString(),
					to: day2End.toISOString(),
					incremental: false
				}
			}]);
		});

		it('Should execute initial load for client received in payload', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'customClient' }]);

			await DataLakeLoad({
				clientCode: 'customClient',
				entity: 'order',
				incremental: false,
				from: '2026-01-01 00:00:00',
				to: '2026-01-02 23:59:59'
			});

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order.lastIncrementalLoadDate'],
				filters: { status: 'active', code: 'customClient' }
			});

			const day1Start = new Date(2026, 0, 1, 0, 0, 0, 0);
			const day1End = new Date(2026, 0, 1, 23, 59, 59, 999);
			const day2Start = new Date(2026, 0, 2, 0, 0, 0, 0);
			const day2End = new Date(2026, 0, 2, 23, 59, 59, 999);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					from: day1Start.toISOString(),
					to: day1End.toISOString(),
					incremental: false
				}
			}, {
				content: {
					entity: 'order',
					from: day2Start.toISOString(),
					to: day2End.toISOString(),
					incremental: false
				}
			}]);
		});

		it('Should send one message per day with correct from unit now when to is not provided', async () => {

			const from = new Date(fakeCurrentDate);
			from.setDate(from.getDate() - 3);

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await DataLakeLoad({
				entity: 'order',
				incremental: false,
				from: from.toISOString()
			});

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order.lastIncrementalLoadDate'],
				filters: { status: 'active' }
			});

			const expectedMessages = [];

			while(from <= fakeCurrentDate) {

				const dayStart = new Date(from);
				dayStart.setHours(0, 0, 0, 0);
				const dayEnd = new Date(from);
				dayEnd.setHours(23, 59, 59, 999);

				expectedMessages.push({
					content: {
						entity: 'order',
						from: dayStart.toISOString(),
						to: dayEnd.toISOString(),
						incremental: false
					}
				});

				from.setDate(from.getDate() + 1);
			}

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, expectedMessages);
		});

		it('Should call publishEvents twice when range spans 51 days (batch of 50 then 1)', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await DataLakeLoad({
				entity: 'order',
				incremental: false,
				from: '2026-01-01 00:00:00',
				to: '2026-02-20 23:59:59'
			});

			const expectedFirstBatch = [];
			const currentDate = new Date('2026-01-01 00:00:00');
			for(let i = 0; i < 50; i++) {
				currentDate.setHours(0, 0, 0, 0);
				const dayStart = new Date(currentDate);
				const dayEnd = new Date(currentDate);
				dayEnd.setHours(23, 59, 59, 999);
				expectedFirstBatch.push({
					content: {
						entity: 'order',
						from: dayStart.toISOString(),
						to: dayEnd.toISOString(),
						incremental: false
					}
				});
				currentDate.setDate(currentDate.getDate() + 1);
			}

			currentDate.setHours(0, 0, 0, 0);
			const day51Start = new Date(currentDate);
			const day51End = new Date(currentDate);
			day51End.setHours(23, 59, 59, 999);
			const expectedSecondBatch = [{
				content: {
					entity: 'order',
					from: day51Start.toISOString(),
					to: day51End.toISOString(),
					incremental: false
				}
			}];

			sinon.assert.calledTwice(SqsEmitter.prototype.publishEvents);
			sinon.assert.calledWithExactly(SqsEmitter.prototype.publishEvents.firstCall, QUEUE_URL, expectedFirstBatch);
			sinon.assert.calledWithExactly(SqsEmitter.prototype.publishEvents.secondCall, QUEUE_URL, expectedSecondBatch);
		});

		it('Should send messages with optional limit and maxSizeMB when in payload', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await DataLakeLoad({
				entity: 'order',
				incremental: false,
				from: '2026-01-01 00:00:00',
				to: '2026-01-01 23:59:59',
				limit: 500,
				maxSizeMB: 200
			});

			const dayStart = new Date(2026, 0, 1, 0, 0, 0, 0);
			const dayEnd = new Date(2026, 0, 1, 23, 59, 59, 999);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					from: dayStart.toISOString(),
					to: dayEnd.toISOString(),
					incremental: false,
					limit: 500,
					maxSizeMB: 200
				}
			}]);
		});
	});
});
