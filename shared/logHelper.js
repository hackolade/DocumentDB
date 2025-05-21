const createLogger = ({ title, logger, hiddenKeys }) => {
	return {
		info(message, infoTitle) {
			logger.log('info', message, infoTitle || title, hiddenKeys);
		},

		progress(message, dbName = '', tableName = '') {
			logger.progress({ message, containerName: dbName, entityName: tableName });
		},

		error(error) {
			logger.log('error', createError(error), title);
		},
	};
};

const createError = error => {
	const message = error.message || error.msg || error.errmsg;

	return {
		message,
		code: error.code,
		stack: error.stack,
	};
};

module.exports = {
	createLogger,
};
