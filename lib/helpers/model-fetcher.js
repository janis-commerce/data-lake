'use strict';

const path = require('path');

module.exports = class ModelFetcher {

	/**
     * Returns an instance model from the service.
     * @param {string} entity
	  * @returns {Model}
	  * @throws {Error} if the model is not found
     */
	static get(entity) {

		const modelPath = this.getModelRelativePath(entity);

		try {
			// eslint-disable-next-line global-require, import/no-dynamic-require
			return require(modelPath);
		} catch(e) {
			throw new Error(`Invalid Model ${entity}. Must be in ${modelPath}.js`);
		}
	}

	static getModelRelativePath(entity) {
		/* istanbul ignore next */
		return path.join(process.cwd(), process.env.MS_PATH || '', 'models', entity);
	}
};
