const applyToInstanceHelper = require("./helpers/applyToInstanceHelper");
const scriptHelper = require("./helpers/scriptHelper");

module.exports = {
	generateContainerScript(data, logger, callback, app) {
		try {
			const _ = app.require('lodash');
			const insertSamplesOption = _.get(data, 'options.additionalOptions', []).find(option => option.id === 'INCLUDE_SAMPLES') || {};
			const withSamples = data.options.origin !== 'ui';
			const useDb = scriptHelper.useDbStatement(data.containerData);
			let script = useDb + '\n\n' + data.entities.map(entityId => {
				const entityData = data.entityData[entityId];
				const script = scriptHelper.getScript({
					entityData,
					containerData: data.containerData,
					jsonSchema: JSON.parse(data.jsonSchema[entityId]),
					definitions: {
						internal: JSON.parse(data.internalDefinitions[entityId]),
						model: JSON.parse(data.modelDefinitions),
						external: JSON.parse(data.externalDefinitions),
					},
				});

				if (entityData?.[0]?.isActivated) {
					return script;
				}

				return `/*\n${script}\n*/`
			}).join('\n\n');
				
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
			logger.log('error', error, 'DocumentDB forward engineering error');
			callback(error);
		}
	},
	generateScript(data, logger, callback, app) {
		try {
			const useDb = scriptHelper.useDbStatement(data.containerData);
			const script = scriptHelper.getScript({
				containerData: data.containerData,
				entityData: data.entityData,
				jsonSchema: JSON.parse(data.jsonSchema),
				definitions: {
					model: JSON.parse(data.modelDefinitions),
					internal: JSON.parse(data.internalDefinitions),
					external: JSON.parse(data.externalDefinitions),
				},
			});
			const samples = scriptHelper.insertSample({
				containerData: data.containerData,
				entityData: data.entityData,
				sample: data.jsonData,
			});

			return callback(null, [useDb, script, samples].join('\n\n'));
		} catch (e) {
			const error = { message: e.message, stack: e.stack };
			logger.log('error', error, 'DocumentDB forward engineering error');
			callback(error);
		}
	},
	applyToInstance: applyToInstanceHelper.applyToInstance,

	testConnection: applyToInstanceHelper.testConnection,
};
