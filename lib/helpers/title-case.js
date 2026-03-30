'use strict';

/**
 * Converts a string to title case: only letters, no spaces, each word capitalized (PascalCase).
 * Non-letter characters are removed; word boundaries are inferred from non-letter positions.
 * @param {string} str - Input string
 * @returns {string} - e.g. "hello world 123 test" -> "HelloWorldTest"
 */
module.exports = str => {
	const words = str
		.replace(/[^a-zA-Z]+/g, ' ')
		.trim()
		.split(/\s+/)
		.filter(Boolean);

	return words
		.map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
		.join('');
};
