'use strict';

/**
 * Converts a string to kebab case: lowercase words separated by hyphens.
 * Non-alphanumeric characters are replaced by hyphens; multiple hyphens are collapsed.
 * @param {string} str - Input string
 * @returns {string} - e.g. "Hello World 123" -> "hello-world-123"
 */
module.exports = str => str
	.replace(/[^a-zA-Z0-9]+/g, '-')
	.replace(/^-|-$/g, '')
	.toLowerCase();
