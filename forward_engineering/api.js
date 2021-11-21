const applyToInstanceHelper = require("./helpers/applyToInstanceHelper");
const scriptHelper = require("./helpers/scriptHelper");

module.exports = {
	generateContainerScript(data, logger, callback, app) {
		try {
			const _ = app.require('lodash');
			const insertSamplesOption = _.get(data, 'options.additionalOptions', []).find(option => option.id === 'INCLUDE_SAMPLES') || {};
			const withSamples = data.options.origin !== 'ui';
			let script = scriptHelper.getScript(data);
			const samples = scriptHelper.insertSamples(data);
			script += withSamples ? '\n' + samples : '';

			if (withSamples || !insertSamplesOption.value) {
				return callback(null, script);
			}


			return callback(null, [
				{ title: 'MongoDB script', script },
				{
					title: 'Sample data',
					script: samples,
				},
			]);
		} catch (e) {
			const error = { message: e.message, stack: e.stack };
			logger.log('error', error, 'CosmosDB w\\ Mongo API forward engineering error');
			callback(error);
		}
	},
	generateScript(data, logger, callback, app) {
		try {
			const script = scriptHelper.getScript(data);
			const samples = scriptHelper.insertSample({
				containerData: data.containerData,
				entityData: data.entityData,
				sample: data.jsonData,
			});

			return callback(null, [script, samples].join('\n\n'));
		} catch (e) {
			const error = { message: e.message, stack: e.stack };
			logger.log('error', error, 'CosmosDB w\\ Mongo API forward engineering error');
			callback(error);
		}
	},
	applyToInstance: applyToInstanceHelper.applyToInstance,

	testConnection: applyToInstanceHelper.testConnection,
};
