# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.7.0] - 2026-04-29
### Changed
- Incremental load: when both `settings.<entity>.lastIncrementalLoadDate` and `settings.<entity>.initialLoad.dateStart` are set, the later date is used (instead of always preferring `lastIncrementalLoadDate`)

## [1.6.4] - 2026-04-28
### Fixed
- Fixed resuming initial load by `_id` from a previous execution

## [1.6.3] - 2026-04-27
### Fixed
- Allow resuming initial load by `_id` from a previous execution

## [1.6.2] - 2026-04-27
### Fixed
- Added missing environment variable for DataLakeLoad SQS queue url

## [1.6.1] - 2026-04-27
### Fixed
- Fixed client settings `initialLoad.dateStart` and `initialLoad.dateEnd` dates
- Using `initialLoad.dateStart` for first incremental execution

## [1.6.0] - 2026-04-27
### Added
- Entity setting `initialLoad.byId: true` to enable initial load paginated by `_id` instead of `dateCreated`, for entities with large collections where `dateCreated` is not indexed
- Entity settings `initialLoad.batchSize` (default `10000`) and `initialLoad.executionLimit` (default `100000`) to control cursor batch size and page size for the `initialLoad.byId` mode
- Consumer auto-paginates `initialLoad.byId` runs: when a page reaches `initialLoad.executionLimit` docs, it re-queues a new SQS message with `lastId` to continue from where it left off
- Entity setting `maxSizeMB` (default `500`) to control the maximum size in MB of the S3 file for the initial load by `dateCreated` mode

## [1.5.0] - 2026-04-16
### Added
- Entity setting `hint` to pass a MongoDB index hint to the consumer model `get()` call

## [1.4.0] - 2026-04-10
### Added
- Entity setting `readPreference` to control the MongoDB read preference used in the consumer model `get()` call. Default: `'secondary'`. Requires **@janiscommerce/mongodb 3.17.0** or higher.

## [1.3.0] - 2026-03-31
### Fixed
- Fixed file descriptor leak (`EMFILE`) in DataLakeLoad Lambda by reusing a single `SqsEmitter` instance across all clients instead of creating one per batch

### Changed
- Optimized initial load message preparation: messages are now built once and reused across all clients instead of being rebuilt for each client

## [1.2.3] - 2026-03-30
### Fixed
- Fixed Schedule name in Serverless resource

## [1.2.2] - 2026-03-26
### Fixed
- Fixed Schedule and ScheduleGroup name to include the service name with serverless variables

## [1.2.1] - 2026-03-26
### Fixed
- Fixed Schedule and ScheduleGroup name to include the service name

## [1.2.0] - 2026-03-26
### Added
- Support for `filenamePrefix` in the sync message to prepend to the S3 object key

## [1.1.0] - 2026-03-26
### Added
- Incremental partitioning: microservices can emit multiple sync messages per client so each consumer run calls the model `get()` with the same date window plus entity-specific filters (via `additionalFilters`).
- Entity setting `excludeFields` to omit fields from the consumer export

### Fixed
- Using `dateModified` sort order for incremental loads

## [1.0.2] - 2026-02-13
### Fixed
- Added missing permissions to put objects in S3

## [1.0.1] - 2026-02-12
### Fixed
- Fixed lambda function passing the environment variable of the sqs queue url

## [1.0.0] - 2026-02-12
### Added
- Initial release
