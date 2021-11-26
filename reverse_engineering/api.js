'use strict';

const connectionHelper = require('./helpers/connectionHelper');
const { createLogger, getSystemInfo } = require('./helpers/logHelper');
const bson = require('bson');

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
			
			const includeSystemCollection = connectionInfo.includeSystemCollection;
			const connection = await connectionHelper.connect(connectionInfo);

			const databases = await connection.getDatabases();

			const result = await async.mapSeries(databases, async (database) => {
				const collections = await connection.getCollections(database.name);
				let dbCollections = collections.map(collection => collection.name);

				log.info({
					message: 'Found collections',
					collections: dbCollections,
				});
				
				dbCollections = await async.filter(dbCollections, async (collectionName) => {
					return connection.hasPermission(database.name, collectionName);
				});

				log.info({
					message: 'Collections that user has access to',
					collections: dbCollections,
				});

				if (!includeSystemCollection) {
					dbCollections = filterSystemCollections(dbCollections);
				}

				log.info({
					message: 'Collections to process',
					collections: dbCollections,
				});

				return {
					dbName: database.name,
					dbCollections,
					isEmpty: dbCollections.length === 0,
				};
			});

			log.info('Names retrieved successfully');

			cb(null, result);
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
			log.progress('Start reverse-engineering');

			const result = await async.reduce(collectionData.dataBaseNames, [], async (result, dbName) => {
				return await async.reduce(collectionData.collections[dbName], result, async (result, collectionName) => {
					log.info({ message: 'Calculate count of documents', dbName, collectionName });
					log.progress('Calculate count of documents', dbName, collectionName);

					const count = await connection.getCount(dbName, collectionName);

					if (!includeEmptyCollection && count === 0) {
						return result;
					}

					log.info({ message: 'Getting documents for sampling', dbName, collectionName });
					log.progress('Getting documents for sampling', dbName, collectionName);

					const limit = getSampleDocSize(count, recordSamplingSettings);
					const documents = await connection.getRandomDocuments(dbName, collectionName, {
						maxTimeMS,
						limit,
						query,
						sort,
					});
					let standardDoc = {};
					
					if (fieldInference.active === 'field') {
						log.info({ message: 'Getting a document for inferring', dbName, collectionName });
						log.progress('Getting a document for inferring', dbName, collectionName);

						standardDoc = await connection.findOne(dbName, collectionName, query);
					}

					log.info({ message: 'Getting indexes', dbName, collectionName });
					log.progress('Getting indexes', dbName, collectionName);

					const indexes = await connection.getIndexes(dbName, collectionName);

					const packageData = {
						dbName,
						collectionName,
						documents: documents.map(serialize),
						relationshipDocuments: filterPotentialForeignKeys(documents).map(serialize),
						primaryKey: '_id',
						typeOfSerializer: 'bson',
						standardDoc: serialize(standardDoc),
						validation: {
							jsonSchema: getJsonSchema(documents[0]),
						},
						entityLevel: {
							Indxs: getIndexes(indexes),
						}
					};

					log.info({ message: 'Collection processed', dbName, collectionName });
					log.progress('Collection processed', dbName, collectionName);

					return result.concat(packageData);
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
	return collections.filter((collectionName) => {
		return collectionName?.length < 8 || collectionName?.substring(0, 7) !== 'system.';
	});
}

function getSampleDocSize(count, recordSamplingSettings) {
	let per = recordSamplingSettings.relative.value;
	return (recordSamplingSettings.active === 'absolute')
		? Math.max(count, recordSamplingSettings.absolute.value)
		: Math.round( count / 100 * per);
}

function getJsonSchema(doc) {
	if (Array.isArray(doc)) {
		const items = getJsonSchema(doc[0]);
		if (items) {
			return {
				items,
			};
		}
	} else if (doc instanceof bson.Long) {
		return {
			type: 'numeric',
			mode: 'integer64'
		};
	} else if (typeof doc === "number" && (doc % 1) !== 0) {
		return {
			type: 'numeric',
			mode: 'double'
		};
	} else if (typeof doc === "number" && Math.abs(doc) < 2**32) {
		return {
			type: 'numeric',
			mode: 'integer32'
		};
	} else if (doc && typeof doc === 'object') {
		const properties = Object.keys(doc).reduce((schema, key) => {
			const data = getJsonSchema(doc[key]);
	
			if (!data) {
				return schema;
			}

			return {
				...schema,
				[key]: data,
			};
		}, {});

		if (Object.keys(properties).length === 0) {
			return;
		}

		return {
			properties,
		};
	}
}

function getIndexes(indexes) {
	return indexes.filter(ind => {
		return ind.name !== '_id_';
	}).map(ind => ({
		name: ind.name,
		key: Object.keys(ind.key).map((key) => {
			return {
				name: key,
				type: getIndexType(ind.key[key]),
			};
		}),
		sparse: ind.sparse,
		unique: ind.unique,
		expireAfterSeconds: ind.expireAfterSeconds,
		"2dsphere": ind['2dsphereIndexVersion'] ? getGeoIndexVersion(ind['2dsphereIndexVersion']) : {}
	}));
}

const getIndexType = (indexType) => {
	switch (indexType) {
		case 1:
			return 'ascending';
		case -1:
			return 'descending';
		case '2dsphere':
			return '2DSphere';
	}

	return 'ascending';
};

const getGeoIndexVersion = (version) => {
	let indexVersion = 'default';

	if (version === 1) {
		indexVersion = 'Version 1';
	}

	if (version === 2) {
		indexVersion = 'Version 2';
	}
	
	return {
		'2dsphereIndexVersion': indexVersion,
	};
};
