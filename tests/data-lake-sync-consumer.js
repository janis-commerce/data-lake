/* eslint-disable max-len */
/* eslint-disable max-classes-per-file */

'use strict';

const assert = require('assert');
const sinon = require('sinon');

const { Readable } = require('stream');

const { SQSHandler } = require('@janiscommerce/sqs-consumer');
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');
const Model = require('@janiscommerce/model');
const Settings = require('@janiscommerce/settings');

const { mockClient } = require('aws-sdk-client-mock');
const {
	S3Client,
	CreateMultipartUploadCommand,
	UploadPartCommand,
	CompleteMultipartUploadCommand
} = require('@aws-sdk/client-s3');

const s3Mock = mockClient(S3Client);
s3Mock.on(CreateMultipartUploadCommand).resolves({ UploadId: '1' });
s3Mock.on(UploadPartCommand).resolves({ ETag: '1' });
s3Mock.on(CompleteMultipartUploadCommand).resolves({});

const { DataLakeSyncConsumer } = require('../lib');

const ModelFetcher = require('../lib/helpers/model-fetcher');

describe('DataLakeSyncConsumer', () => {

	const clientCode = 'defaultClient';

	const sqsQueueArn = 'arn:aws:sqs:us-east-1:000000000000:DataLakeSyncQueue';

	const QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/123/data-lake-sync';

	const originalEnv = { ...process.env };

	let settingsGetStub;
	let publishEventsStub;

	const ProductModel = class extends Model {};
	const OrderModel = class extends Model {};
	const ReservationsModel = class extends Model {};
	const StockModel = class extends Model {};
	const PickingModel = class extends Model {};

	const createEvent = records => {
		return {
			Records: records.map((body, index) => ({
				messageId: `e10d3743-34ff-4bc0-a3f7-${index.toString().padStart(12, '0')}`,
				receiptHandle: `e10d37${index.toString().padStart(10, '0')}`,
				eventSourceARN: sqsQueueArn,
				messageAttributes: {
					'janis-client': { stringValue: clientCode }
				},
				body: JSON.stringify(body)
			}))
		};
	};

	beforeEach(() => {

		settingsGetStub = sinon.stub(Settings, 'get').returns({
			entities: [
				{ name: 'product', fields: ['id', 'name', 'price'] },
				{ name: 'order', excludeFields: ['items'] },
				{ name: 'reservations', readPreference: 'secondaryPreferred' },
				{ name: 'stock', hint: { dateModified: 1, warehouse: 1 } },
				{ name: 'picking', hint: 'dateModified_1_warehouse_1' }
			]
		});

		publishEventsStub = sinon.stub(SqsEmitter.prototype, 'publishEvents').resolves({ failedCount: 0 });

		process.env.AWS_REGION = 'us-east-1';
		process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || 'test';
		process.env.AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || 'test';

		process.env.DATA_LAKE_SYNC_ROLE_ARN = sqsQueueArn;
		process.env.S3_DATA_LAKE_RAW_BUCKET = 'test-bucket';
		process.env.JANIS_SERVICE_NAME = 'test-service';
	});

	afterEach(() => {
		sinon.restore();
		s3Mock.reset();
		process.env = { ...originalEnv };
	});

	it('Should process message getting from entity and upload to S3 file when mode is incremental', async () => {

		sinon.stub(ModelFetcher, 'get').returns(OrderModel);

		const fakeCursor = {
			batchSize() { return this; },
			stream() {
				const r = new Readable({ objectMode: true });
				r.push({ _id: { toString: () => '507f1f77bcf86cd799439011' }, code: 'product1' });
				r.push(null);
				return r;
			}
		};

		sinon.stub(OrderModel.prototype, 'get').resolves(fakeCursor);

		await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
			entity: 'order',
			mode: 'incremental',
			from: '2026-01-01 00:00:00',
			to: '2026-01-02 23:59:59'
		}]));

		sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'order');

		sinon.assert.calledOnceWithExactly(OrderModel.prototype.get, {
			excludeFields: ['items'],
			order: { dateModified: 'asc' },
			filters: {
				dateModifiedFrom: new Date('2026-01-01 00:00:00'),
				dateModifiedTo: new Date('2026-01-02 23:59:59')
			},
			returnType: 'cursor',
			readPreference: 'secondary'
		});

		sinon.assert.calledOnceWithExactly(Settings.get, 'dataLake');

		const createCalls = s3Mock.calls(CreateMultipartUploadCommand);

		assert.strictEqual(createCalls.length, 1);
		assert.strictEqual(createCalls[0].args[0].input.Bucket, 'test-bucket');
		assert.ok(createCalls[0].args[0].input.Key.startsWith('microservice=test-service/entity=order/load_type=incremental/client_code=defaultClient/'));
	});

	it('Should process message with additional filters when mode is incremental', async () => {

		sinon.stub(ModelFetcher, 'get').returns(ProductModel);

		const fakeCursor = {
			batchSize() { return this; },
			stream() {
				const r = new Readable({ objectMode: true });
				r.push({ _id: { toString: () => '507f1f77bcf86cd799439011' }, code: 'product1' });
				r.push(null);
				return r;
			}
		};

		sinon.stub(ProductModel.prototype, 'get').resolves(fakeCursor);

		await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
			entity: 'product',
			mode: 'incremental',
			from: '2026-01-01 00:00:00',
			to: '2026-01-02 23:59:59',
			additionalFilters: { category: 'electronics' }
		}]));

		sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'product');

		sinon.assert.calledOnceWithExactly(ProductModel.prototype.get, {
			fields: ['id', 'name', 'price'],
			order: { dateModified: 'asc' },
			filters: {
				dateModifiedFrom: new Date('2026-01-01 00:00:00'),
				dateModifiedTo: new Date('2026-01-02 23:59:59'),
				category: 'electronics'
			},
			returnType: 'cursor',
			readPreference: 'secondary'
		});

		sinon.assert.calledOnceWithExactly(Settings.get, 'dataLake');

		const createCalls = s3Mock.calls(CreateMultipartUploadCommand);

		assert.strictEqual(createCalls.length, 1);
		assert.strictEqual(createCalls[0].args[0].input.Bucket, 'test-bucket');
		assert.ok(createCalls[0].args[0].input.Key.startsWith('microservice=test-service/entity=product/load_type=incremental/client_code=defaultClient/'));
	});

	it('Should process message when mode is initial (date range)', async () => {

		sinon.stub(ModelFetcher, 'get').returns(ReservationsModel);

		const fakeCursor = {
			batchSize() { return this; },
			stream() {
				const r = new Readable({ objectMode: true });
				r.push({ _id: { toString: () => '507f1f77bcf86cd799439011' }, code: 'product1' });
				r.push(null);
				return r;
			}
		};

		sinon.stub(ReservationsModel.prototype, 'get').resolves(fakeCursor);

		await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
			entity: 'reservations',
			mode: 'initial',
			from: '2026-01-01 00:00:00',
			to: '2026-01-02 23:59:59'
		}]));

		sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'reservations');

		sinon.assert.calledOnceWithExactly(ReservationsModel.prototype.get, {
			order: { dateCreated: 'asc' },
			filters: {
				dateCreatedFrom: new Date('2026-01-01 00:00:00'),
				dateCreatedTo: new Date('2026-01-02 23:59:59')
			},
			returnType: 'cursor',
			readPreference: 'secondaryPreferred'
		});

		sinon.assert.calledOnceWithExactly(Settings.get, 'dataLake');

		const createCalls = s3Mock.calls(CreateMultipartUploadCommand);

		assert.strictEqual(createCalls.length, 1);
		assert.strictEqual(createCalls[0].args[0].input.Bucket, 'test-bucket');
		assert.ok(createCalls[0].args[0].input.Key.startsWith('microservice=test-service/entity=reservations/load_type=initial/client_code=defaultClient/'));
	});

	it('Should pass hint as object to model get() when entity settings include hint as object', async () => {

		sinon.stub(ModelFetcher, 'get').returns(StockModel);

		const fakeCursor = {
			batchSize() { return this; },
			stream() {
				const r = new Readable({ objectMode: true });
				r.push({ _id: { toString: () => '507f1f77bcf86cd799439011' }, code: 'stock1' });
				r.push(null);
				return r;
			}
		};

		sinon.stub(StockModel.prototype, 'get').resolves(fakeCursor);

		await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
			entity: 'stock',
			mode: 'incremental',
			from: '2026-01-01 00:00:00',
			to: '2026-01-02 23:59:59'
		}]));

		sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'stock');

		sinon.assert.calledOnceWithExactly(StockModel.prototype.get, {
			hint: { dateModified: 1, warehouse: 1 },
			order: { dateModified: 'asc' },
			filters: {
				dateModifiedFrom: new Date('2026-01-01 00:00:00'),
				dateModifiedTo: new Date('2026-01-02 23:59:59')
			},
			returnType: 'cursor',
			readPreference: 'secondary'
		});

		sinon.assert.calledOnceWithExactly(Settings.get, 'dataLake');

		const createCalls = s3Mock.calls(CreateMultipartUploadCommand);

		assert.strictEqual(createCalls.length, 1);
		assert.strictEqual(createCalls[0].args[0].input.Bucket, 'test-bucket');
		assert.ok(createCalls[0].args[0].input.Key.startsWith('microservice=test-service/entity=stock/load_type=incremental/client_code=defaultClient/'));
	});

	it('Should pass hint as string to model get() when entity settings include hint as index name', async () => {

		sinon.stub(ModelFetcher, 'get').returns(PickingModel);

		const fakeCursor = {
			batchSize() { return this; },
			stream() {
				const r = new Readable({ objectMode: true });
				r.push({ _id: { toString: () => '507f1f77bcf86cd799439011' }, code: 'picking1' });
				r.push(null);
				return r;
			}
		};

		sinon.stub(PickingModel.prototype, 'get').resolves(fakeCursor);

		await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
			entity: 'picking',
			mode: 'incremental',
			from: '2026-01-01 00:00:00',
			to: '2026-01-02 23:59:59'
		}]));

		sinon.assert.calledOnceWithExactly(ModelFetcher.get, 'picking');

		sinon.assert.calledOnceWithExactly(PickingModel.prototype.get, {
			hint: 'dateModified_1_warehouse_1',
			order: { dateModified: 'asc' },
			filters: {
				dateModifiedFrom: new Date('2026-01-01 00:00:00'),
				dateModifiedTo: new Date('2026-01-02 23:59:59')
			},
			returnType: 'cursor',
			readPreference: 'secondary'
		});

		sinon.assert.calledOnceWithExactly(Settings.get, 'dataLake');

		const createCalls = s3Mock.calls(CreateMultipartUploadCommand);

		assert.strictEqual(createCalls.length, 1);
		assert.strictEqual(createCalls[0].args[0].input.Bucket, 'test-bucket');
		assert.ok(createCalls[0].args[0].input.Key.startsWith('microservice=test-service/entity=picking/load_type=incremental/client_code=defaultClient/'));
	});

	it('Should not process when invalid message received', async () => {

		sinon.spy(ModelFetcher, 'get');

		await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
			from: '2026-01-01 00:00:00',
			to: '2026-01-02 23:59:59'
		}]));

		sinon.assert.notCalled(ModelFetcher.get);
		sinon.assert.notCalled(Settings.get);
	});

	describe('Initial load by id', () => {

		const ShipmentModel = class extends Model {};
		const ClientModel = class extends Model {};

		beforeEach(() => {
			settingsGetStub.returns({
				entities: [
					{ name: 'shipment', initialLoad: { byId: true, batchSize: 2, executionLimit: 4 } }
				]
			});
			process.env.DATA_LAKE_SYNC_SQS_QUEUE_URL = QUEUE_URL;

			const modelFetcherStub = sinon.stub(ModelFetcher, 'get');
			modelFetcherStub.withArgs('shipment').returns(ShipmentModel);
			modelFetcherStub.withArgs('client').returns(ClientModel);

			sinon.stub(ClientModel.prototype, 'update').resolves();
		});

		it('Should process first execution: single batch smaller than batchSize sets dateStart, dateEnd and lastId', async () => {

			sinon.stub(ShipmentModel.prototype, 'get').resolves([{ id: 'id1', code: 'shipment1' }]);

			const before = new Date();

			await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
				entity: 'shipment',
				mode: 'initialById'
			}]));

			const after = new Date();

			sinon.assert.calledOnceWithExactly(ShipmentModel.prototype.get, {
				order: { id: 'asc' },
				limit: 2,
				readPreference: 'secondary'
			});

			sinon.assert.notCalled(publishEventsStub);

			const [[updateValues, updateFilter]] = ClientModel.prototype.update.args;
			assert.deepStrictEqual(updateFilter, { code: clientCode });
			assert.deepStrictEqual(updateValues.$inc, { 'settings.shipment.initialLoad.totalItems': 1 });
			assert.strictEqual(updateValues['settings.shipment.initialLoad.lastId'], 'id1');
			assert.ok(updateValues['settings.shipment.initialLoad.dateStart'] >= before);
			assert.ok(updateValues['settings.shipment.initialLoad.dateStart'] <= after);
			assert.ok(updateValues['settings.shipment.initialLoad.dateEnd'] >= before);
			assert.ok(updateValues['settings.shipment.initialLoad.dateEnd'] <= after);
		});

		it('Should process continuation: last batch smaller than batchSize sets dateEnd and lastId without dateStart', async () => {

			sinon.stub(ShipmentModel.prototype, 'get').resolves([{ id: 'id99', code: 'shipment99' }]);

			await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
				entity: 'shipment',
				mode: 'initialById',
				lastId: 'prevId'
			}]));

			sinon.assert.calledOnceWithExactly(ShipmentModel.prototype.get, {
				order: { id: 'asc' },
				filters: { id: { type: 'greater', value: 'prevId' } },
				limit: 2,
				readPreference: 'secondary'
			});

			sinon.assert.notCalled(publishEventsStub);

			const [[updateValues, updateFilter]] = ClientModel.prototype.update.args;
			assert.deepStrictEqual(updateFilter, { code: clientCode });
			assert.deepStrictEqual(updateValues.$inc, { 'settings.shipment.initialLoad.totalItems': 1 });
			assert.strictEqual(updateValues['settings.shipment.initialLoad.lastId'], 'id99');
			assert.ok(updateValues['settings.shipment.initialLoad.dateEnd']);
			assert.strictEqual(updateValues['settings.shipment.initialLoad.dateStart'], undefined);
		});

		it('Should re-queue when executionLimit is reached on first execution: sets dateStart and lastId but not dateEnd', async () => {

			sinon.stub(ShipmentModel.prototype, 'get')
				.onFirstCall()
				.resolves([{ id: 'id1', code: 'shipment1' }, { id: 'id2', code: 'shipment2' }])
				.onSecondCall()
				.resolves([{ id: 'id3', code: 'shipment3' }, { id: 'id4', code: 'shipment4' }]);

			await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
				entity: 'shipment',
				mode: 'initialById'
			}]));

			assert.strictEqual(ShipmentModel.prototype.get.callCount, 2);

			sinon.assert.calledWithExactly(ShipmentModel.prototype.get.getCall(0), {
				order: { id: 'asc' },
				limit: 2,
				readPreference: 'secondary'
			});

			sinon.assert.calledWithExactly(ShipmentModel.prototype.get.getCall(1), {
				order: { id: 'asc' },
				filters: { id: { type: 'greater', value: 'id2' } },
				limit: 2,
				readPreference: 'secondary'
			});

			sinon.assert.calledOnceWithExactly(publishEventsStub, QUEUE_URL, [{
				content: {
					entity: 'shipment',
					mode: 'initialById',
					lastId: 'id4'
				}
			}]);

			const [[updateValues, updateFilter]] = ClientModel.prototype.update.args;
			assert.deepStrictEqual(updateFilter, { code: clientCode });
			assert.deepStrictEqual(updateValues.$inc, { 'settings.shipment.initialLoad.totalItems': 4 });
			assert.strictEqual(updateValues['settings.shipment.initialLoad.lastId'], 'id4');
			assert.ok(updateValues['settings.shipment.initialLoad.dateStart']);
			assert.strictEqual(updateValues['settings.shipment.initialLoad.dateEnd'], undefined);
		});

		it('Should re-queue when executionLimit is reached on continuation: sets lastId but not dateStart nor dateEnd', async () => {

			sinon.stub(ShipmentModel.prototype, 'get')
				.onFirstCall()
				.resolves([{ id: 'id3', code: 'shipment3' }, { id: 'id4', code: 'shipment4' }])
				.onSecondCall()
				.resolves([{ id: 'id5', code: 'shipment5' }, { id: 'id6', code: 'shipment6' }]);

			await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
				entity: 'shipment',
				mode: 'initialById',
				lastId: 'id2'
			}]));

			assert.strictEqual(ShipmentModel.prototype.get.callCount, 2);

			sinon.assert.calledOnceWithExactly(publishEventsStub, QUEUE_URL, [{
				content: {
					entity: 'shipment',
					mode: 'initialById',
					lastId: 'id6'
				}
			}]);

			const [[updateValues, updateFilter]] = ClientModel.prototype.update.args;
			assert.deepStrictEqual(updateFilter, { code: clientCode });
			assert.deepStrictEqual(updateValues.$inc, { 'settings.shipment.initialLoad.totalItems': 4 });
			assert.strictEqual(updateValues['settings.shipment.initialLoad.lastId'], 'id6');
			assert.strictEqual(updateValues['settings.shipment.initialLoad.dateStart'], undefined);
			assert.strictEqual(updateValues['settings.shipment.initialLoad.dateEnd'], undefined);
		});

		it('Should not re-queue and set dateEnd when get() returns empty array on continuation', async () => {

			sinon.stub(ShipmentModel.prototype, 'get').resolves([]);

			await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
				entity: 'shipment',
				mode: 'initialById',
				lastId: 'lastPage'
			}]));

			sinon.assert.notCalled(publishEventsStub);

			const [[updateValues, updateFilter]] = ClientModel.prototype.update.args;
			assert.deepStrictEqual(updateFilter, { code: clientCode });
			assert.ok(updateValues['settings.shipment.initialLoad.dateEnd']);
		});

		it('Should upload to S3 with load_type=initial key prefix', async () => {

			sinon.stub(ShipmentModel.prototype, 'get').resolves([{ id: 'id1', code: 'shipment1' }]);

			await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
				entity: 'shipment',
				mode: 'initialById'
			}]));

			const createCalls = s3Mock.calls(CreateMultipartUploadCommand);

			assert.strictEqual(createCalls.length, 1);
			assert.strictEqual(createCalls[0].args[0].input.Bucket, 'test-bucket');
			assert.ok(createCalls[0].args[0].input.Key.startsWith('microservice=test-service/entity=shipment/load_type=initial/client_code=defaultClient/'));
		});

		it('Should respect custom initialLoad settings for batchSize and executionLimit', async () => {

			settingsGetStub.returns({
				entities: [
					{ name: 'shipment', initialLoad: { byId: true, batchSize: 3, executionLimit: 3 } }
				]
			});

			sinon.stub(ShipmentModel.prototype, 'get').resolves([
				{ id: 'id1', code: 'shipment1' },
				{ id: 'id2', code: 'shipment2' },
				{ id: 'id3', code: 'shipment3' }
			]);

			await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
				entity: 'shipment',
				mode: 'initialById'
			}]));

			sinon.assert.calledOnceWithExactly(ShipmentModel.prototype.get, {
				order: { id: 'asc' },
				limit: 3,
				readPreference: 'secondary'
			});

			sinon.assert.calledOnceWithExactly(publishEventsStub, QUEUE_URL, [{
				content: {
					entity: 'shipment',
					mode: 'initialById',
					lastId: 'id3'
				}
			}]);

			const [[updateValues]] = ClientModel.prototype.update.args;
			assert.deepStrictEqual(updateValues.$inc, { 'settings.shipment.initialLoad.totalItems': 3 });
		});

		it('Should use default batchSize and executionLimit when initialLoad settings are not set', async () => {

			settingsGetStub.returns({
				entities: [
					{ name: 'shipment' }
				]
			});

			sinon.stub(ShipmentModel.prototype, 'get').resolves([{ id: 'id1', code: 'shipment1' }]);

			await SQSHandler.handle(DataLakeSyncConsumer, createEvent([{
				entity: 'shipment',
				mode: 'initialById'
			}]));

			sinon.assert.calledOnceWithExactly(ShipmentModel.prototype.get, {
				order: { id: 'asc' },
				limit: 10000,
				readPreference: 'secondary'
			});
		});
	});
});
