'use strict';

const Settings = require('@janiscommerce/settings');

let entitySettings;

module.exports = entity => {

	if(!entitySettings) {
		const dataLakeSettings = Settings.get('dataLake');
		entitySettings = dataLakeSettings?.entities?.find(({ name }) => name === entity);
	}

	return entitySettings;
};
