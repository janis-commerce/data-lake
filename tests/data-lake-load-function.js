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

		const dateStart = '2025-01-01 00:00:00';

		beforeEach(() => {
			settingsGetStub
				.returns({ entities: [{ name: 'order', initialLoad: { byId: true } }] });
		});

		it('Should send message and update client when client has no lastIncrementalLoadDate for entity', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1', settings: { order: { initialLoad: { dateStart } } } }]);

			await DataLakeLoad({ entity: 'order', mode: 'incremental' });

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order'],
				filters: { status: 'active' }
			});

			const from = new Date(dateStart);
			const to = new Date(dateStart);
			to.setDate(to.getDate() + 1);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'incremental',
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

			await DataLakeLoad({ entity: 'order', mode: 'incremental' });

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order'],
				filters: { status: 'active' }
			});

			const from = new Date(lastIncrementalLoadDate);
			const to = new Date(lastIncrementalLoadDate);
			to.setDate(to.getDate() + 1);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'incremental',
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

		it('Should use the newer of lastIncrementalLoadDate and initialLoad.dateStart when both are set', async () => {

			const lastIncrementalLoadDate = '2025-01-01 00:00:00';
			const newerDateStart = '2025-12-01 00:00:00';

			sinon.stub(ClientModel.prototype, 'get').resolves([{
				code: 'client1',
				settings: {
					order: {
						lastIncrementalLoadDate,
						initialLoad: { dateStart: newerDateStart }
					}
				}
			}]);

			await DataLakeLoad({ entity: 'order', mode: 'incremental' });

			const from = new Date(newerDateStart);
			const to = new Date(newerDateStart);
			to.setDate(to.getDate() + 1);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'incremental',
					from,
					to
				}
			}]);
		});

		it('Should send message and update client when client has lastIncrementalLoadDate for entity and is today', async () => {

			const lastIncrementalLoadDate = new Date();
			lastIncrementalLoadDate.setHours(lastIncrementalLoadDate.getHours() - 3);

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1', settings: { order: { lastIncrementalLoadDate } } }]);

			await DataLakeLoad({ entity: 'order', mode: 'incremental' });

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order'],
				filters: { status: 'active' }
			});

			const from = new Date(lastIncrementalLoadDate);
			const to = new Date(); // limited to now

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'incremental',
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

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1', settings: { order: { initialLoad: { dateStart } } } }]);

			await DataLakeLoad({ entity: 'order', mode: 'incremental' });

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order'],
				filters: { status: 'active' }
			});

			const from = new Date(dateStart);
			const to = new Date(dateStart);
			to.setDate(to.getDate() + 1);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'incremental',
					from,
					to
				}
			}]);

			sinon.assert.notCalled(ClientModel.prototype.update);
		});

		it('Should throw when initialLoad.dateFrom is missing from payload', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			settingsGetStub
				.returns({ entities: [{ name: 'order' }] });

			await assert.rejects(() => DataLakeLoad({ entity: 'order', mode: 'incremental' }));
		});
	});

	describe('Initial load', () => {

		beforeEach(() => {
			settingsGetStub
				.returns({ entities: [{ name: 'order', initialLoad: { dateFrom: '2025-01-01 00:00:00' } }] });
		});

		afterEach(() => {
			sinon.assert.notCalled(ClientModel.prototype.update);
		});

		it('Should send one message per day per client with correct from/to and mode initial', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }, { code: 'client2' }]);

			await DataLakeLoad({
				entity: 'order',
				from: '2026-01-01 00:00:00',
				to: '2026-01-02 23:59:59'
			});

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order'],
				filters: { status: 'active' }
			});

			const day1Start = new Date(2026, 0, 1, 0, 0, 0, 0);
			const day1End = new Date(2026, 0, 1, 23, 59, 59, 999);
			const day2Start = new Date(2026, 0, 2, 0, 0, 0, 0);
			const day2End = new Date(2026, 0, 2, 23, 59, 59, 999);

			sinon.assert.calledTwice(SqsEmitter.prototype.publishEvents);
			sinon.assert.calledWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'initial',
					from: day1Start.toISOString(),
					to: day1End.toISOString()
				}
			}, {
				content: {
					entity: 'order',
					mode: 'initial',
					from: day2Start.toISOString(),
					to: day2End.toISOString()
				}
			}]);
		});

		it('Should execute initial load for client received in payload', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'customClient' }]);

			await DataLakeLoad({
				clientCode: 'customClient',
				entity: 'order',
				from: '2026-01-01 00:00:00',
				to: '2026-01-02 23:59:59'
			});

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order'],
				filters: { status: 'active', code: 'customClient' }
			});

			const day1Start = new Date(2026, 0, 1, 0, 0, 0, 0);
			const day1End = new Date(2026, 0, 1, 23, 59, 59, 999);
			const day2Start = new Date(2026, 0, 2, 0, 0, 0, 0);
			const day2End = new Date(2026, 0, 2, 23, 59, 59, 999);

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'initial',
					from: day1Start.toISOString(),
					to: day1End.toISOString()
				}
			}, {
				content: {
					entity: 'order',
					mode: 'initial',
					from: day2Start.toISOString(),
					to: day2End.toISOString()
				}
			}]);
		});

		it('Should send one message per day with correct from until now when to is not provided', async () => {

			const from = new Date(fakeCurrentDate);
			from.setDate(from.getDate() - 3);

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await DataLakeLoad({
				entity: 'order',
				from: from.toISOString()
			});

			sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'client');
			sinon.assert.calledOnceWithExactly(ClientModel.prototype.get, {
				fields: ['code', 'settings.order'],
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
						mode: 'initial',
						from: dayStart.toISOString(),
						to: dayEnd.toISOString()
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
						mode: 'initial',
						from: dayStart.toISOString(),
						to: dayEnd.toISOString()
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
					mode: 'initial',
					from: day51Start.toISOString(),
					to: day51End.toISOString()
				}
			}];

			sinon.assert.calledTwice(SqsEmitter.prototype.publishEvents);
			sinon.assert.calledWithExactly(SqsEmitter.prototype.publishEvents.firstCall, QUEUE_URL, expectedFirstBatch);
			sinon.assert.calledWithExactly(SqsEmitter.prototype.publishEvents.secondCall, QUEUE_URL, expectedSecondBatch);
		});

		it('Should throw when from is missing', async () => {

			settingsGetStub
				.returns({ entities: [{ name: 'order' }] });

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await assert.rejects(() => DataLakeLoad({ entity: 'order' }));

			sinon.assert.notCalled(SqsEmitter.prototype.publishEvents);
		});

		it('Should throw when from is greater than to (date from in payload)', async () => {

			const dateFrom = new Date();
			dateFrom.setDate(dateFrom.getDate() + 5);

			settingsGetStub
				.returns({ entities: [{ name: 'order' }] });

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await assert.rejects(() => DataLakeLoad({ entity: 'order', from: dateFrom.toISOString() }));
		});

		it('Should throw when from is greater than to (date from in settings)', async () => {

			const dateFrom = new Date();
			dateFrom.setDate(dateFrom.getDate() + 5);

			settingsGetStub
				.returns({ entities: [{ name: 'order', initialLoad: { dateFrom: dateFrom.toISOString() } }] });

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await assert.rejects(() => DataLakeLoad({ entity: 'order' }));
		});
	});

	describe('Initial load by id', () => {

		beforeEach(() => {
			settingsGetStub.returns({ entities: [{ name: 'order', initialLoad: { byId: true } }] });
		});

		afterEach(() => {
			sinon.assert.notCalled(ClientModel.prototype.update);
		});

		it('Should send a single message with mode initialById and no dates', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await DataLakeLoad({ entity: 'order' });

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'initialById'
				}
			}]);
		});

		it('Should send a single message with mode initialById and lastId (resume initial load from lastId)', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1', settings: { order: { initialLoad: { lastId: 'id1' } } } }]);

			await DataLakeLoad({ entity: 'order' });

			sinon.assert.calledOnceWithExactly(SqsEmitter.prototype.publishEvents, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'initialById',
					lastId: 'id1'
				}
			}]);
		});

		it('Should send a single message with mode initialById for multiple clients', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }, { code: 'client2' }]);

			await DataLakeLoad({ entity: 'order' });

			sinon.assert.calledTwice(SqsEmitter.prototype.publishEvents);
			sinon.assert.calledWithExactly(SqsEmitter.prototype.publishEvents.firstCall, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'initialById'
				}
			}]);
			sinon.assert.calledWithExactly(SqsEmitter.prototype.publishEvents.secondCall, QUEUE_URL, [{
				content: {
					entity: 'order',
					mode: 'initialById'
				}
			}]);
		});

		it('Should not require from/to dates and not throw', async () => {

			sinon.stub(ClientModel.prototype, 'get').resolves([{ code: 'client1' }]);

			await assert.doesNotReject(() => DataLakeLoad({ entity: 'order' }));
		});
	});
});
