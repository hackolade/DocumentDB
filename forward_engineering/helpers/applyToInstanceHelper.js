const vm = require('vm');
const bson = require('../../reverse_engineering/node_modules/bson');
const connectionHelper = require('../../reverse_engineering/helpers/connectionHelper');
const loggerHelper = require('../../reverse_engineering/helpers/logHelper');

const applyToInstanceHelper = {
	async applyToInstance(data, logger, cb) {
		const log = loggerHelper.createLogger({
			title: 'Applying to instance',
			hiddenKeys: data.hiddenKeys,
			logger,
		});

		try {
			logger.clear();
			log.info(loggerHelper.getSystemInfo(data.appVersion));
			log.info(data);

			const connection = await connectionHelper.connect(data);

			const mongodbScript = replaceUseCommand(convertBson(data.script));
			await runMongoDbScript({
				mongodbScript,
				logger: log,
				connection
			});

			connectionHelper.close();
			cb(null);
		} catch (error) {
			log.error(error);
			connectionHelper.close();

			cb({
				message: error.message,
				stack: error.stack,
			});
		}
	},

	async testConnection(connectionInfo, logger, cb){
		const log = loggerHelper.createLogger({
			title: 'Test connection',
			hiddenKeys: connectionInfo.hiddenKeys,
			logger,
		});

		try {
			logger.clear();
			log.info(loggerHelper.getSystemInfo(connectionInfo.appVersion));
			log.info(connectionInfo);
	
			await connectionHelper.connect(connectionInfo);
			connectionHelper.close();

			log.info('Connected successfully');
	
			cb();
		} catch (error) {
			log.error(error);
			
			return cb({
				message: error.message,
				stack: error.stack,
			});
		}
	},
};

const replaceUseCommand = (script) => {
	return script.split('\n').filter(Boolean).map(line => {
		const useStatement = /^use\ ([\s\S]+);$/i;
		const result = line.match(useStatement);

		if (!result) {
			return line;
		}

		return `useDb("${result[1]}");`;
	}).join('\n');
};

const runMongoDbScript = ({ mongodbScript, logger: loggerInstance, connection }) => {
	let currentDb;
	let commands = [];
	const logger = createProgressLog(loggerInstance);

	logger.info('Start applying instance ...');

	const context = {
		ISODate: (d) => new Date(d),
		ObjectId: bson.ObjectId,
		Binary: bson.Binary,
		BinData: (i, data) => bson.Binary(data),
		MinKey: bson.MinKey,
		MaxKey: bson.MaxKey,
		Code: bson.Code,
		Timestamp: bson.Timestamp,

		useDb(dbName) {
			currentDb = dbName;
		},

		db: {
			createCollection(collectionName) {
				const command = () => connection.createCollection(currentDb, collectionName).then(() => {
					logger.info(`Collection ${collectionName} created`);
				}, (error) => {
					const errMessage = `Collection ${collectionName} not created`;
					logger.error(error, errMessage);
					error.message = errMessage;

					return Promise.reject(error);
				});

				commands.push(command);
			},
			getCollection(collectionName) {
				const collection = connection.getCollection(currentDb, collectionName);

				return {
					createIndex(fields, params = {}) {
						const command = () => collection.createIndex(fields, params).then(() => {
							logger.info(`index ${params.name} created`);
						}, (error) => {
							const errMessage = `index ${params.name} not created`;
							logger.error(error, errMessage);
							error.message = errMessage;

							return Promise.reject(error);
						});

						commands.push(command);
					},
					insert(data) {
						const command = () => collection.insertOne(data).then(() => {
							logger.info(`sample inserted`);
						}).catch(error => {
							const errMessage = `sample is not inserted: ${error.message}`;
							logger.error(error, errMessage);
							error.message = errMessage;

							return Promise.reject(error);
						});

						commands.push(command);
					}
				};
			}
		}
	};

	vm.createContext(context);
	vm.runInContext(mongodbScript, context);

	return commands.reduce((prev, next) => {
		return prev.then(() => next());
	}, Promise.resolve());
};

const createProgressLog = logger => ({
	info(message) {
		logger.progress(`${message}`);
		logger.info(message);
	},
	error(error, message) {
		logger.progress( `[color:red]failed: ${message}`);
		logger.error(error);
	},
	warning(message, error) {
		logger.progress({
			message: `[color:orange]warning: ${message}`,
		});
		if (error) {
			logger.error(error);
		}
	}
});

function convertBson(sample) {
	return sample.replace(/\{\s*\"\$minKey\": (\d*)\s*\}/gi, 'MinKey($1)')
		.replace(/\{\s*\"\$maxKey\": (\d*)\s*\}/gi, 'MaxKey($1)');
}

module.exports = applyToInstanceHelper;
