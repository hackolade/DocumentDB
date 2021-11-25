'use strict';

const connectionHelper = require('./helpers/connectionHelper');
const { createLogger, getSystemInfo } = require('./helpers/logHelper');
const bson = require('bson');

const ERROR_CONNECTION = 1;
const ERROR_DB_LIST = 2;
const ERROR_DB_CONNECTION = 3;
const ERROR_LIST_COLLECTION = 4;
const ERROR_GET_DATA = 5;
const ERROR_HANDLE_BUCKET = 6;
const ERROR_COLLECTION_DATA = 7;

module.exports = {
	disconnect: function(connectionInfo, logger, cb) {
		connectionHelper.close();
		cb()
	},

	async testConnection(connectionInfo, logger, cb){
		const log = createLogger({
			title: 'Test connection',
			hiddenKeys: connectionInfo.hiddenKeys,
			logger,
		});

		try {
			logger.clear();
			log.info(getSystemInfo(connectionInfo.appVersion));
			log.info(connectionInfo);

			await connectionHelper.connect(connectionInfo);

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

	async getDbCollectionsNames(connectionInfo, logger, cb, app) {
		const log = createLogger({
			title: 'Retrieving databases and collections information',
			hiddenKeys: connectionInfo.hiddenKeys,
			logger,
		});

		try {
			const _ = app.require('lodash');
			const async = app.require('async');
			
			logger.clear();
			log.info(getSystemInfo(connectionInfo.appVersion));
			log.info(connectionInfo);
			
			const connection = await connectionHelper.connect(connectionInfo);

			const databases = await connection.getDatabases();

			async.mapSeries(databases, (database, next) => {
				connection.getCollections(database.name).then(
					collections => {
						const dbCollections = collections.map(collection => collection.name);

						next(null, {
							dbName: database.name,
							dbCollections,
							isEmpty: dbCollections.length === 0,
						});
					},
					error => next(error),
				);
			}, (error, result) => {
				if (error) {
					log.error(error);
					cb({ message: error.message, stack: error.stack });
					return;
				}

				log.info('Names retrieved successfully');

				cb(null, result);
			});
		} catch (error) {
			log.error(error);
			cb({ message: error.message, stack: error.stack });
		}
	},

	async getDbCollectionsData(data, logger, cb, app){
		const log = createLogger({
			title: 'Retrieving data for inferring schema',
			hiddenKeys: data.hiddenKeys,
			logger,
		});

		try {
			const async = app.require('async');	
			const _ = app.require('lodash');
			const { recordSamplingSettings, fieldInference, includeEmptyCollection, collectionData } = data;
			const query = safeParse(data.queryCriteria);
			const sort = safeParse(data.sortCriteria);
			const maxTimeMS = Number(data.queryRequestTimeout) || 120000;

			log.info({
				title: 'Parameters',
				sampling: getSamplingInfo(recordSamplingSettings, fieldInference),
				data,
			});

			const connection = await connectionHelper.connect();
			const dbInfo = await connection.getBuildInfo();
			const modelInfo = {
				version: dbInfo.version,
			};

			const result = await async.reduce(collectionData.dataBaseNames, [], async (result, dbName) => {
				return await async.reduce(collectionData.collections[dbName], result, async (result, collectionName) => {
					const count = await connection.getCount(dbName, collectionName);
					const limit = getSampleDocSize(count, recordSamplingSettings);
					const documents = await connection.getRandomDocuments(dbName, collectionName, {
						maxTimeMS,
						limit,
						query,
						sort,
					});

					return result.concat({
						dbName,
						collectionName,
						documents: documents.map(serialize),
						relationshipDocuments: filterPotentialForeignKeys(documents).map(serialize),
						primaryKey: '_id',
						typeOfSerializer: 'bson',
					});
				});
			});

			cb(null, result, modelInfo, []);
		} catch (error) {
			log.error(error);
			cb({ message: error.message, stack: error.stack });
		}
	}
};

const serialize = (data) => (new bson()).serialize(data);

const safeParse = (data) => {
	try {
		return JSON.parse(data);
	} catch (e) {
		return {};
	}
};

const filterPotentialForeignKeys = (documents) => {
	const isObject = (item) => item && typeof item === 'object';
	const isEmpty = (item) => {
		if (!item) {
			return true;
		}

		if (Array.isArray(item)) {
			return item.length === 0;
		}

		if (isObject(item)) {
			return Object.keys(item).length === 0;
		}

		return false;
	};

	const iterateProps = document => {
		return Object.keys(document).reduce((obj, prop) => {
			if (document?.[prop]?._bsontype === 'ObjectID') {
				return { ...obj, [prop]: document[prop] };
			} else if (document?.[prop]?._bsontype === 'DBRef') {
				return { ...obj };
			}

			if (isObject(document[prop]) || Array.isArray(document[prop])) {
				const innerData = iterateProps(document[prop]);
				if (isEmpty(innerData)) {
					return { ...obj };
				}
				return { ...obj, [prop]: innerData };
			}

			return { ...obj };
		}, {});
	};

	return documents.map(document => iterateProps(document)).filter((item) => !isEmpty(item));
};

function getSamplingInfo(recordSamplingSettings, fieldInference){
	let samplingInfo = {};
	let value = recordSamplingSettings[recordSamplingSettings.active].value;
	let unit = (recordSamplingSettings.active === 'relative') ? '%' : ' records max';
	samplingInfo.recordSampling = `${recordSamplingSettings.active} ${value}${unit}`
	samplingInfo.fieldInference = (fieldInference.active === 'field') ? 'keep field order' : 'alphabetical order';
	return samplingInfo;
}

function filterSystemCollections(collections) {
	const listExcludedCollections = ["system.indexes"];

	return collections.filter((collection) => {
		return collection.name.length < 8 || collection.name.substring(0, 7) !== 'system.';
	});
}

function getSampleDocSize(count, recordSamplingSettings) {
	let per = recordSamplingSettings.relative.value;
	return (recordSamplingSettings.active === 'absolute')
		? Math.max(count, recordSamplingSettings.absolute.value)
		: Math.round( count / 100 * per);
}
