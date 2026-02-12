# Data Lake

[![Build Status](https://github.com/janis-commerce/data-lake/workflows/Build%20Status/badge.svg)](https://github.com/janis-commerce/data-lake/actions)
[![Coverage Status](https://coveralls.io/repos/github/janis-commerce/data-lake/badge.svg?branch=master)](https://coveralls.io/github/janis-commerce/data-lake?branch=master)

Package to send data from Janis microservices to the **Janis Data Lake** service.

## What it does

The package orchestrates **initial** and **incremental** syncs of entity data: a scheduled Lambda enqueues load messages per client; an SQS consumer reads those messages, queries the service model by date range, and uploads the results to S3 as compressed NDJSON (`.ndjson.gz`). The Data Lake then ingests from that raw bucket.

**Flow:** EventBridge Scheduler → **DataLakeLoad** Lambda → SQS → **DataLakeSync** Consumer → S3 (raw bucket).

## Installation

```bash
npm install @janiscommerce/data-lake
```

> **Warning:** The consumer uses `returnType: 'cursor'` on the model `get()`. Your service must use **@janiscommerce/mongodb** version **3.14.0** or higher, where cursor support was added.

## Usage in a Janis service

To enable Data Lake sync in a microservice, follow these steps.

---

### 1. Add the hooks

Register the Data Lake hooks in your Serverless config so the Lambda, SQS queue, consumer, schedules, and IAM roles are created.

You need to pass `SQSHelper` from `sls-helper-plugin-janis` into the hooks.

**File: `serverless.js` (or where you define `helper`)**

```js
'use strict';

const { helper } = require('sls-helper');
const { SQSHelper } = require('sls-helper-plugin-janis');
const { dataLakeServerlessHelperHooks } = require('@janiscommerce/data-lake');

module.exports = helper({
	hooks: [
		// ... other hooks (api, functions, etc.)
		...dataLakeServerlessHelperHooks(SQSHelper)
	]
});
```

**What this adds:**

- Lambda **DataLakeLoad** (handler: `src/lambda/DataLakeLoad/index.js`)
- SQS queue and consumer Lambda for **dataLakeSync** (handler: `src/sqs-consumer/data-lake-sync-consumer.js`)
- EventBridge Schedule Group and one Schedule per entity (from Settings), invoking the Lambda with `incremental: true` and `entity` in kebab-case
- IAM role for the scheduler to invoke the Lambda

---

### 2. Add the DataLakeLoad Lambda

Create the Lambda handler that extends the package class. It must live at the path expected by the hooks: **`src/lambda/DataLakeLoad/index.js`**.

**File: `src/lambda/DataLakeLoad/index.js`**

```js
'use strict';

const { Handler } = require('@janiscommerce/lambda');
const { DataLakeLoadFunction } = require('@janiscommerce/data-lake');

module.exports.handler = (...args) => Handler.handle(DataLakeLoadFunction, ...args);

```

**Payload:** The Lambda receives a payload with `entity`, `incremental`, and optionally `from`, `to`, `limit`, `maxSizeMB`. The schedules send only `{ incremental: true, entity: "<entity-kebab-case>" }`. For an **initial load** you must invoke the Lambda manually with `entity`, `incremental: false`, and `from` (and optionally `to`, `limit`, `maxSizeMB`).

---

### 3. Add the Data Lake Sync consumer

Create the SQS consumer file at the path expected by the hooks: **`src/sqs-consumer/data-lake-sync-consumer.js`**.

**File: `src/sqs-consumer/data-lake-sync-consumer.js`**

```js
'use strict';

const { SQSHandler } = require('@janiscommerce/sqs-consumer');
const { DataLakeSyncConsumer } = require('@janiscommerce/data-lake');

module.exports.handler = event => SQSHandler.handle(DataLakeSyncConsumer, event);

```

This re-exports the package consumer handler. The consumer reads messages from the Data Lake sync queue, loads data from your model (by `entity` and date range), and uploads NDJSON.gz to S3.

**Options:** The consumer uses `IterativeSQSConsumer` (one record per invocation when `batchSize: 1`).

---

### 4. Add settings for entities to sync

Configure which entities are synced and how often (in minutes). Settings are read from your service config (e.g. `environments/{environment}/.janiscommercerc.json` by `@janiscommerce/settings`).

**File: `src/environments/{environment}/.janiscommercerc.json`**

```json
{
	"dataLake": {
		"entities": [{
			"name": "order",
			"frequency": 60,
			"initialLoadDate": "2025-01-01 00:00:00"
		}, {
			"name": "product",
			"frequency": 120,
			"initialLoadDate": "2025-01-01 00:00:00"
		}]
	}
}
```

**Entity options:**

| Property          | Type   | Required | Description |
|-------------------|--------|----------|-------------|
| `name`            | string | Yes      | Entity name in **kebab-case**. Must match the model path: the package loads the model from `models/<name>.js` (e.g. `order` → `models/order.js`). This same value is sent in the payload as `entity`. |
| `frequency`       | number | No       | How often (in minutes) to run the incremental sync. Default: `60`. Used in the Schedule expression: `rate(<frequency> minutes)`. |
| `initialLoadDate` | string | Yes      | Valid date string (e.g. `YYYY-MM-DD HH:mm:ss` or ISO). Used when the client has no `settings.<entity>.lastIncrementalLoadDate` for this entity (e.g. first run or new clients). Required so incremental sync can compute the date range. |
| `fields`          | array  | No       | If set, only these fields are requested from the model in the consumer (reduces payload size and control what goes to the Data Lake). |

**Example with `fields` and `initialLoadDate`:**

```json
{
	"dataLake": {
		"entities": [
			{
				"name": "order",
				"frequency": 30,
				"initialLoadDate": "2025-01-01 00:00:00",
				"fields": ["id", "commerceId", "total", "status"]
			}
		]
	}
}
```

**Important:** `dataLake.entities` is **required**. If it is missing, the hooks will throw at load time: `dataLake.entities is required in Settings file`. Each entity must have a model at `models/<name>.js` and the model must support `get()` with filters `dateCreatedFrom`/`dateCreatedTo` (initial) or `dateModifiedFrom`/`dateModifiedTo` (incremental) and `returnType: 'cursor'`.

---

### Manual execution: initial load

To run an **initial load** (full export by date range) you must invoke the **DataLakeLoad** Lambda manually. The payload must include the entity and the start date; end date and client are optional (default: all active clients, up to today).

**Payload:**

| Field        | Type    | Required | Description |
|--------------|---------|----------|-------------|
| `entity`     | string  | Yes      | Entity name in kebab-case (e.g. `order`, `product`). |
| `incremental`| boolean | Yes      | Use `false` for initial load. |
| `from`       | string  | Yes      | Start date (valid date string, e.g. `YYYY-MM-DD HH:mm:ss` or ISO). |
| `to`         | string  | No       | End date. Default: today end of day. |
| `clientCode` | string  | No       | If set, only this client is synced; otherwise all active clients. |
| `limit`      | number  | No       | Optional limit passed to the consumer. |
| `maxSizeMB`  | number  | No       | Optional max size per file (MB) in the consumer. |

**Example: all clients, from a given date to today**

```json
{
  "body": {
    "entity": "order",
    "incremental": false,
    "from": "2025-01-01 00:00:00"
  }
}
```

**Example: with end date and a single client**

```json
{
  "body": {
    "entity": "product",
    "incremental": false,
    "from": "2025-01-01 00:00:00",
    "to": "2025-06-30 23:59:59",
    "clientCode": "my-client-code"
  }
}
```

---

## Environment variables

- **`DATA_LAKE_SYNC_SQS_QUEUE_URL`** – Set by the hooks (SQS queue URL for the sync queue). The DataLakeLoad Lambda publishes messages here.
- **`S3_DATA_LAKE_RAW_BUCKET`** – Set by the hooks on the consumer Lambda (e.g. `janis-data-lake-service-raw-${stage}`). The consumer uploads NDJSON.gz files here.