'use strict';

const { createGzip } = require('zlib');
const { PassThrough } = require('stream');
const { Upload } = require('@aws-sdk/lib-storage');

/**
 * Streams gzip-compressed NDJSON lines to S3 multipart uploads.
 * Handles file rotation when the in-memory chunk exceeds maxSizeBytes.
 */
module.exports = class NdjsonGzipS3Uploader {

	constructor({
		s3Client,
		bucket,
		keyPrefix,
		filenamePart,
		filenamePrefix,
		pushedAt,
		maxSizeBytes,
		logger,
		clientCode,
		entity
	}) {

		this.s3Client = s3Client;
		this.bucket = bucket;
		this.keyPrefix = keyPrefix;
		this.filenamePart = filenamePart;
		this.filenamePrefix = filenamePrefix || '';
		this.pushedAt = pushedAt;
		this.maxSizeBytes = maxSizeBytes;
		this.logger = logger;
		this.clientCode = clientCode;
		this.entity = entity;

		this.partIndex = 0;
		this.currentBytes = 0;
		this.currentPassThrough = null;
		this.uploadPromises = [];
	}

	startNewPart() {

		this.partIndex++;
		this.currentBytes = 0;
		this.currentPassThrough = new PassThrough();
		const gzip = createGzip({ level: 6 });

		const prefixSegment = this.filenamePrefix ? `${this.filenamePrefix}-` : '';
		const key = `${this.keyPrefix}/${prefixSegment}${this.filenamePart}-${this.pushedAt}-${String(this.partIndex).padStart(3, '0')}.ndjson.gz`;

		this.logger.info(`[${this.clientCode} - ${this.entity}] Starting new S3 upload part ${this.partIndex} → ${key}`);

		const uploader = new Upload({
			client: this.s3Client,
			params: {
				Bucket: this.bucket,
				Key: key,
				Body: this.currentPassThrough.pipe(gzip),
				ContentType: 'application/gzip'
			}
		});

		uploader.on('httpUploadProgress', progress => {
			if(progress?.total)
				this.logger.debug(`[${this.clientCode} - ${this.entity}] Upload part ${this.partIndex} progress: ${progress.loaded}/${progress.total}`);
		});

		this.uploadPromises.push(uploader.done());
	}

	rotateIfNeeded(lineSize) {

		if(!this.currentPassThrough || (this.currentBytes + lineSize) > this.maxSizeBytes) {
			if(this.currentPassThrough)
				this.currentPassThrough.end();
			this.startNewPart();
		}
	}

	/**
	 * Async consumer path: awaits drain when the buffer is full.
	 */
	async writeLine(line) {

		const chunk = Buffer.isBuffer(line) ? line : Buffer.from(line, 'utf8');
		const lineSize = chunk.length;

		this.rotateIfNeeded(lineSize);
		this.currentBytes += lineSize;

		const canContinue = this.currentPassThrough.write(chunk);
		if(!canContinue)
			await new Promise(resolve => { this.currentPassThrough.once('drain', resolve); });
	}

	/**
	 * Stream consumer path: returns false when upstream should pause until drain.
	 */
	writeLineWithBackpressure(line) {

		const chunk = Buffer.isBuffer(line) ? line : Buffer.from(line, 'utf8');
		const lineSize = chunk.length;

		this.rotateIfNeeded(lineSize);
		this.currentBytes += lineSize;

		return this.currentPassThrough.write(chunk);
	}

	onNextDrain(callback) {

		this.currentPassThrough.once('drain', callback);
	}

	async finalize() {

		if(this.currentPassThrough)
			this.currentPassThrough.end();

		await Promise.all(this.uploadPromises);
	}
};
