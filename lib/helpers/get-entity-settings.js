'use strict';

const Settings = require('@janiscommerce/settings');

module.exports = entity => {
	const dataLakeSettings = Settings.get('dataLake');
	return dataLakeSettings?.entities?.find(({ name }) => name === entity);
};
