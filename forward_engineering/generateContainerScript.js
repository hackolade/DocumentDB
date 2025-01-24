const _ = require('lodash');
const scriptHelper = require('./helpers/scriptHelper');

const generateContainerScript = (data, logger, callback) => {
	try {
		const insertSamplesOption =
			_.get(data, 'options.additionalOptions', []).find(option => option.id === 'INCLUDE_SAMPLES') || {};
		const withSamples = data.options.origin !== 'ui';
		const useDb = scriptHelper.useDbStatement(data.containerData);
		let script =
			useDb +
			'\n\n' +
			data.entities
				.map(entityId => {
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

					return `/*\n${script}\n*/`;
				})
				.join('\n\n');

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
};

module.exports = {
	generateContainerScript,
};
