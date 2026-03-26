# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
