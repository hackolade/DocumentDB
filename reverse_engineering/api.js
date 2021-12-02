'use strict';

const connectionHelper = require('./helpers/connectionHelper');
const { createLogger, getSystemInfo } = require('./helpers/logHelper');
const bson = require('bson');
const awsHelper = require('./helpers/awsHelper');

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
			const awsSdk = app.require('aws-sdk');

			awsHelper({
				...connectionInfo,
				...parseHost(connectionInfo.host, log)
			}, awsSdk);

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
			const awsConnection = awsHelper();

			log.info({
				title: 'Parameters',
				sampling: getSamplingInfo(recordSamplingSettings, fieldInference),
				data,
			});

			const connection = await connectionHelper.connect();
			const dbInfo = await connection.getBuildInfo();
			let modelInfo = {
				version: dbInfo.version,
			};
			log.progress('Start reverse-engineering');

			try {
				log.info('Getting cluster information');
				log.progress('Getting cluster information ...');

				const cluster = await awsConnection.getCluster();
				if (!cluster) {
					throw new Error('Cluster doesn\'t exist in the chosen region.');
				}
				const tags = await awsConnection.tags(cluster['DBClusterArn']);
				const clusterData = getClusterData(cluster);
				modelInfo = {
					...modelInfo,
					'source-region': awsConnection.getRegion(),
					...clusterData,
					tags: tags['TagList']?.map(tag => ({
						tagName: tag['Key'],
						tagValue: tag['Value'],
					})),
				};
			} catch (e) {
				log.progress('[color:red]Error of getting cluster information');
				log.info('Cannot get information about the cluster from AWS. Please, check AWS credentials in the connection settings.');
				log.error(e);
			}
			

			const result = await async.reduce(collectionData.dataBaseNames, [], async (result, dbName) => {
				return await async.reduce(collectionData.collections[dbName], result, async (result, collectionName) => {
					log.info({ message: 'Calculate count of documents', dbName, collectionName });
					log.progress('Calculate count of documents', dbName, collectionName);

					const count = await connection.getCount(dbName, collectionName);

					if (!includeEmptyCollection && count === 0) {
						return result;
					}

					const limit = getSampleDocSize(count, recordSamplingSettings);

					log.info({ message: 'Getting documents for sampling', dbName, collectionName, countOfDocuments: count, documentsToSample: limit });
					log.progress('Getting documents for sampling', dbName, collectionName);

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

const serialize = (data) => {
	const b = new bson();
	
	if (!data) {
		data = {};
	}

	return b.serialize(data)
};

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
	const limit = (recordSamplingSettings.active === 'absolute')
		? Math.min(count, recordSamplingSettings.absolute.value)
		: Math.ceil( count / 100 * per);

	return Math.min(recordSamplingSettings.maxValue || 10000, limit);
}

function getJsonSchema(doc) {
	if (doc instanceof bson.ObjectID || doc instanceof bson.DBRef) {
		return;
	} else if (Array.isArray(doc)) {
		const items = getJsonSchema(doc[0]);
		if (items) {
			return {
				items,
			};
		}
	} else if (doc instanceof bson.Long) {
		return {
			type: 'numeric',
			mode: 'int64'
		};
	} else if (typeof doc === "number" && (doc % 1) !== 0) {
		return {
			type: 'numeric',
			mode: 'double'
		};
	} else if (typeof doc === "number" && Math.abs(doc) < 2**32) {
		return {
			type: 'numeric',
			mode: 'int32'
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

const getClusterData = (cluster) => {
	return {
		dbInstances: cluster['DBClusterMembers']?.map(instance => ({
			dbInstanceIdentifier: instance['DBInstanceIdentifier'],
			dbInstanceRole: instance['IsClusterWriter'] ? 'Primary' : 'Replica',
		})),
		DBClusterIdentifier: cluster['DBClusterIdentifier'],
		DBClusterArn: cluster['DBClusterArn'],
		Endpoint: cluster['Endpoint'],
		ReaderEndpoint: cluster['ReaderEndpoint'],
		MultiAZ: cluster['MultiAZ'],
		Port: cluster['Port'] ?? 27017,
		DBClusterParameterGroup: cluster['DBClusterParameterGroup'],
		DbClusterResourceId: cluster['DbClusterResourceId'],
		StorageEncrypted: cluster['StorageEncrypted'],
		BackupRetentionPeriod: String(cluster['BackupRetentionPeriod'] ?? ''),
		auditLog: false,
		maintenance: Boolean(cluster['PreferredMaintenanceWindow']),
		...(cluster['PreferredMaintenanceWindow'] ? parseMaintenance(cluster['PreferredMaintenanceWindow']) : {}),
	};
};

const parseMaintenance = (period) => {
	const getDay = (d) => {
		const day = d.slice(0, 3);

		return ({
			sun: 'Sunday',
			mon: 'Monday',
			tue: 'Tuesday',
			wed: 'Wednesday',
			thu: 'Thursday',
			fri: 'Friday',
			sat: 'Saturday',
		})[day] || 'Sunday';
	};
	const getTime = (d) => {
		const time = d.slice(4);
		const [hours, minutes] = time.split(':');
		const date = new Date(0);

		date.setHours(+hours);
		date.setMinutes(+minutes);

		return date;
	};

	const [from, to] = period.split('-');
	
	return {
		startDay: getDay(from),
		startHour: String(getTime(from).getHours()).padStart(2, '0'),
		startMinute: String(getTime(from).getMinutes()).padStart(2, '0'),
		duration: String((getTime(to) - getTime(from)) / (1000 * 60 * 60)),
	};
};

const parseHost = (host, logger) => {
	try {
		const [dbClusterIdentifier,,region] = host.split('.');
	
		return {
			dbClusterIdentifier,
			region,
		};
	} catch (e) {
		logger.error(e);
	}
};
