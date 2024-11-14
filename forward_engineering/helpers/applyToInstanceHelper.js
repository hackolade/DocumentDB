const vm = require('vm');
const bson = require('bson');
const connectionHelper = require('../../shared/mongoDbClient');
const { createLogger, getSystemInfo } = require('../../shared/logHelper');
const readNdJsonByLine = require('./ndJsonHelper');

const applyToInstanceHelper = {
	async applyToInstance(data, logger, cb, app) {
		const sshService = app.require('@hackolade/ssh-service');
		const log = createLogger({
			title: 'Applying to instance',
			hiddenKeys: data.hiddenKeys,
			logger,
		});

		try {
			logger.clear();
			log.info(getSystemInfo(data.appVersion));
			log.info(data);

			const connection = await connectionHelper.connect(data, sshService);

			const { scriptWithSamples, numberOfSamples } = await generateScriptForInsertingDataInBulk(
				data.script,
				data.entitiesData,
				log,
			);
			const mongodbScript = replaceUseCommand(convertBson(scriptWithSamples));
			await runMongoDbScript({
				mongodbScript,
				logger: log,
				connection,
				numberOfSamples,
			});

			connectionHelper.close(sshService);
			cb(null);
		} catch (error) {
			log.error(error);
			connectionHelper.close(sshService);
			cb(error);
		}
	},

	async testConnection(connectionInfo, logger, cb, app) {
		const sshService = app.require('@hackolade/ssh-service');
		const log = createLogger({
			title: 'Test connection',
			hiddenKeys: connectionInfo.hiddenKeys,
			logger,
		});

		try {
			logger.clear();
			log.info(getSystemInfo(connectionInfo.appVersion));
			log.info(connectionInfo);

			await connectionHelper.connect(connectionInfo, sshService);
			connectionHelper.close(sshService);

			log.info('Connected successfully');

			cb();
		} catch (error) {
			log.error(error);
			return cb(error);
		}
	},
};

const replaceUseCommand = script => {
	return script
		.split('\n')
		.filter(Boolean)
		.map(line => {
			const useStatement = /^use ([\s\S]+);$/i;
			const result = line.match(useStatement);

			if (!result) {
				return line;
			}

			return `useDb("${result[1]}");`;
		})
		.join('\n');
};

const runMongoDbScript = ({ mongodbScript, logger: loggerInstance, connection, numberOfSamples }) => {
	let currentDb;
	let commands = [];
	let insertedSamples = 0;
	let prevInsertingProgress = 0;
	const logger = createProgressLog(loggerInstance);

	const createConnectionHandler = ({ collectionName }) => {
		const postProcess = () => logger.info(`Collection ${collectionName} created`);
		const errorHandler = error => {
			const errMessage = `Collection ${collectionName} not created`;
			logger.error(error, errMessage);
			error.message = errMessage;

			return Promise.reject(new Error(error));
		};

		const command = () => connection.createCollection(currentDb, collectionName).then(postProcess, errorHandler);
		commands.push(command);
	};

	const getCollectionHandler = ({ collectionName }) => {
		const collection = connection.getCollection(currentDb, collectionName);

		return {
			createIndex(fields, params = {}) {
				const postProcess = () => logger.info(`index ${params.name} created`);
				const errorHandler = error => {
					const errMessage = `index ${params.name} not created`;
					logger.error(error, errMessage);
					error.message = errMessage;

					return Promise.reject(new Error(error));
				};

				const command = () => collection.createIndex(fields, params).then(postProcess, errorHandler);
				commands.push(command);
			},
			insert(data) {
				const postProcess = () => {
					insertedSamples++;
					const insertingProgress = Math.round((insertedSamples / numberOfSamples) * 100);
					if (insertingProgress - prevInsertingProgress < 5) {
						return;
					}
					prevInsertingProgress = insertingProgress;

					logger.info(`Inserting Samples: ${insertingProgress}%`);
				};
				const errorHandler = error => {
					insertedSamples++;
					const errMessage = `sample is not inserted ${insertedSamples} / ${numberOfSamples} Reason: ${error.message}`;
					logger.error(error, errMessage);
					error.message = errMessage;

					return Promise.reject(new Error(error));
				};

				const command = () => collection.insertOne(data).then(postProcess).catch(errorHandler);
				commands.push(command);
			},
		};
	};

	logger.info('Start applying instance ...');

	const context = {
		ISODate: d => new Date(d),
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
				createConnectionHandler({ collectionName });
			},
			getCollection(collectionName) {
				getCollectionHandler({ collectionName });
			},
		},
	};

	vm.createContext(context);
	vm.runInContext(mongodbScript, context);

	return commands.reduce((prev, next) => {
		return prev.then(() => next());
	}, Promise.resolve());
};

const generateScriptForInsertingDataInBulk = async (script, entitiesData, logger) => {
	let numberOfSamples = Object.keys(entitiesData).length;
	const scriptWithSamples = await Object.values(entitiesData).reduce(
		async (resultScript, entityData) => {
			resultScript = await resultScript;

			if (!entityData.filePath) {
				return Promise.resolve(resultScript);
			}

			try {
				const collectionName = entityData.code || entityData.name;
				const documents = await readNdJsonByLine(entityData.filePath, logger);
				numberOfSamples += documents.length;

				return (
					resultScript +
					documents.map(document => `db.getCollection("${collectionName}").insert(${document});`).join('\n\n')
				);
			} catch (error) {
				logger.error(error, 'Error during publishing fake data in bulk');
			}
		},
		Promise.resolve(script + '\n\n'),
	);

	return { scriptWithSamples, numberOfSamples };
};

const createProgressLog = logger => ({
	info(message) {
		logger.progress(`${message}`);
		logger.info(message);
	},
	error(error, message) {
		logger.progress(`[color:red]failed: ${message}`);
		logger.error(error);
	},
	warning(message, error) {
		logger.progress({
			message: `[color:orange]warning: ${message}`,
		});
		if (error) {
			logger.error(error);
		}
	},
});

function convertBson(sample) {
	return sample
		.replace(/\{\s*"\$minKey": (\d*)\s*}/gi, 'MinKey($1)')
		.replace(/\{\s*"\$maxKey": (\d*)\s*}/gi, 'MaxKey($1)');
}

module.exports = applyToInstanceHelper;
