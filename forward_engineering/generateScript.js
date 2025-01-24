const scriptHelper = require('./helpers/scriptHelper');

const generateScript = (data, logger, callback) => {
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
};

module.exports = {
	generateScript,
};
