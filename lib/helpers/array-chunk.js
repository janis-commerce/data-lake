'use strict';

/**
 * Chunk an array into smaller arrays of a given size.
 * @param {Array} arr - The array to chunk.
 * @param {Number} size - The size of each chunk.
 * @returns {Array} - An array of chunks.
 */
module.exports = (arr, size) => {

	if(arr.length <= size)
		return [arr];

	// Pre-calculate number of chunks to avoid dynamic array growth
	const chunksCount = Math.ceil(arr.length / size);
	const chunks = new Array(chunksCount);

	for(let i = 0; i < chunksCount; i++) {
		const start = i * size;
		const end = Math.min(start + size, arr.length);
		chunks[i] = arr.slice(start, end);
	}

	return chunks;
};
