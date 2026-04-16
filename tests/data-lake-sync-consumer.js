/* eslint-disable max-len */
/* eslint-disable max-classes-per-file */

'use strict';

const assert = require('assert');
const sinon = require('sinon');

const { Readable } = require('stream');

const { SQSHandler } = require('@janiscommerce/sqs-consumer');
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

	const originalEnv = { ...process.env };

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

		sinon.stub(Settings, 'get').returns({
			entities: [
				{ name: 'product', fields: ['id', 'name', 'price'] },
				{ name: 'order', excludeFields: ['items'] },
				{ name: 'reservations', readPreference: 'secondaryPreferred' },
				{ name: 'stock', hint: { dateModified: 1, warehouse: 1 } },
				{ name: 'picking', hint: 'dateModified_1_warehouse_1' }
			]
		});

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

	it('Should process message getting from entity and upload to S3 file when incremental is true', async () => {

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
			incremental: true,
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

	it('Should process message getting from entity and upload to S3 file when incremental is true with additional filters', async () => {

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
			incremental: true,
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

	it('Should process message getting from entity and upload to S3 file when incremental is false (initial load)', async () => {

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
			incremental: false,
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
			incremental: true,
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
			incremental: true,
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
});
