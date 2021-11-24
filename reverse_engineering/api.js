'use strict';

const connectionHelper = require('./helpers/connectionHelper');
const bson = require('bson');

const ERROR_CONNECTION = 1;
const ERROR_DB_LIST = 2;
const ERROR_DB_CONNECTION = 3;
const ERROR_LIST_COLLECTION = 4;
const ERROR_GET_DATA = 5;
const ERROR_HANDLE_BUCKET = 6;
const ERROR_COLLECTION_DATA = 7;

module.exports = {
	connect(connectionInfo, logger) {
		logger.clear();
		logger.log('info', connectionInfo, 'Reverse-Engineering connection settings', connectionInfo.hiddenKeys);

		return connectionHelper.connect(connectionInfo);
	},

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
			logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);

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
			logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);
			
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

	getDbCollectionsData: function(data, logger, cb, app){
		const async = app.require('async');	
		let includeEmptyCollection = data.includeEmptyCollection;
		let { recordSamplingSettings, fieldInference } = data;
		logger.progress = logger.progress || (() => {});
		logger.log('info', getSamplingInfo(recordSamplingSettings, fieldInference), 'Reverse-Engineering sampling params', data.hiddenKeys);

		let bucketList = data.collectionData.dataBaseNames;

		logger.log('info', { CollectionList: bucketList }, 'Selected collection list', data.hiddenKeys);

		const cosmosClient = new CosmosClient(data.database, data.host, data.password, data.isLocal);

		this.connect(data, logger, async (err, connection) => {
			if (err) {
				logger.progress({ message: 'Error of connecting to the instance.\n ' + err.message, containerName: data.database, entityName: '' });											
				logger.log('error', err);
				return cb(err);
			}
			
			try {
				const db = connection.db(data.database);

				if (!db) {
					connection.close();
					let error = `Failed connection to database ${data.database}`;
					logger.log('error', error);
					logger.progress({ message: 'Error of connecting to the database .\n ' + data.database, containerName: data.database, entityName: '' });											
					return cb(createError(ERROR_DB_CONNECTION, error));
				}

				let modelInfo = {
					accountID: data.password,
					version: '4.0.0'
				};

				await Promise.all([
					getBuildInfo(db).catch(err => {
						logger.progress({ message: 'Error while getting version: ' + err.message, containerName: data.database, entityName: '' });
						logger.log('error', err);
					}),
					data.includeAccountInformation
						? cosmosClient.getAdditionalAccountInfo(data).catch(err => {
							logger.progress({ message: 'Error while getting control pane data: ' + err.message, containerName: data.database, entityName: '' });
							logger.log('error', err, 'Error while getting control pane data');

							return {};
						})
						: Promise.resolve({}),
				]).then(([ buildInfo, controlPaneData ]) => {
					modelInfo = {
						...modelInfo,
						...controlPaneData,
						apiExperience: 'Mongo API',
						version: buildInfo.version,
					}
				});

				async.map(bucketList, (bucketName, collItemCallback) => {
					const collection = db.collection(bucketName);
					logger.progress({ message: 'Collection data loading ...', containerName: data.database, entityName: bucketName });											

					getBucketInfo(db, bucketName, (err) => {
						logger.progress({ message: 'Error of getting collection data .\n ' + err.message, containerName: data.database, entityName: bucketName });											
						logger.log('error', err);
					}, (err, bucketInfo = {}) => {
						if (err) {
							logger.progress({ message: 'Error of getting collection data .\n ' + err.message, containerName: data.database, entityName: bucketName });											
							logger.log('error', err);
						}

						bucketInfo = {
							...bucketInfo,
							dbId: data.database,
						};

						logger.progress({ message: 'Collection data has loaded', containerName: data.database, entityName: bucketName });											
						logger.progress({ message: 'Loading documents...', containerName: data.database, entityName: bucketName });											

						getData(collection, recordSamplingSettings, (err, documents) => {
							if(err) {
								logger.progress({ message: 'Error of loading documents.\n ' + err.message, containerName: data.database, entityName: bucketName });											
								logger.log('error', err);
								return collItemCallback(err, null);
							} else {
								logger.progress({ message: 'Documents have loaded', containerName: data.database, entityName: bucketName });											

								documents  = filterDocuments(documents);
								let documentKindName = data.documentKinds[bucketName].documentKindName || '*';
								let docKindsList = data.collectionData.collections[bucketName];
								let collectionPackages = [];										

								if (documentKindName !== '*') {
									if(!docKindsList) {
										if (includeEmptyCollection) {
											let documentsPackage = {
												dbName: bucketName,
												emptyBucket: true,
												indexes: [],
												bucketIndexes: [],
												views: [],
												validation: false,
												bucketInfo
											};

											collectionPackages.push(documentsPackage)
										}
									} else {
										docKindsList.forEach(docKindItem => {
											let newArrayDocuments = documents.filter((item) => {
												return item[documentKindName] == docKindItem;
											});

											let documentsPackage = {
												dbName: bucketName,
												collectionName: String(docKindItem),
												documents: adjustDocuments(newArrayDocuments || []),
												indexes: [],
												bucketIndexes: [],
												views: [],
												validation: {
													jsonSchema: getJsonSchema(newArrayDocuments[0]),
												},
												docType: documentKindName,
												bucketInfo
											};

											if(fieldInference.active === 'field') {
												documentsPackage.documentTemplate = newArrayDocuments[0] || null;
											}
											if (documentsPackage.documents.length > 0 || includeEmptyCollection) {
												collectionPackages.push(documentsPackage)
											}
										});
									}
								} else {
									let documentsPackage = {
										dbName: bucketName,
										collectionName: bucketName,
										documents: adjustDocuments(documents || []),
										indexes: [],
										bucketIndexes: [],
										views: [],
										validation: {
											jsonSchema: getJsonSchema(documents[0]),
										},
										docType: bucketName,
										bucketInfo
									};

									if(fieldInference.active === 'field'){
										documentsPackage.documentTemplate = documents[0] || null;
									}
									if (documentsPackage.documents.length > 0 || includeEmptyCollection) {
										collectionPackages.push(documentsPackage);
									}
								}

								return collItemCallback(err, collectionPackages);
							}
						});
					});
				}, (err, items) => {
					if(err){
						logger.log('error', err);
					}
					connection.close();
					return cb(createError(ERROR_COLLECTION_DATA, err), items, modelInfo);
				});
			} catch (err) {
				if(err){
					logger.log('error', err);
				}
				connection.close();
				return cb(createError(ERROR_COLLECTION_DATA, err));
			}
		});
	}
};

const getValue = (doc) => {
	if (doc instanceof bson.BSONRegExp || doc instanceof RegExp) {
		return doc.toString();
	} else if (doc instanceof Date) {
		return doc.toISOString();
	} else if (doc instanceof bson.ObjectID) {
		return `ObjectId("${doc.toString()}")`;
	} else if (doc instanceof bson.MinKey) {
		return '';
	} else if (doc instanceof bson.MaxKey) {
		return '';
	} else if (doc instanceof bson.Code) {
		return doc.code;
	} else if (doc instanceof bson.Decimal128) {
		return 1.0;
	} else if (doc instanceof bson.Long) {
		return 1;
	} else if (doc instanceof bson.Binary) {
		return doc.buffer.toString('base64'); 
	} else if (typeof doc === 'number' && Math.abs(doc) > 2**32) {
		return Math.abs(doc) % 2**32;
	}
};

function adjustDocuments(doc) {
	if (Array.isArray(doc)) {
		return doc.map(adjustDocuments);
	} else if (getValue(doc) !== undefined) {
		return getValue(doc);
	} else if (doc && typeof doc === 'object') {
		return Object.keys(doc).reduce((result, key) => {
			return {
				...result,
				[key]: adjustDocuments(doc[key]),
			};
		}, {});
	} else {
		return doc;
	}
}

function getJsonSchema(doc) {
	if (Array.isArray(doc)) {
		const items = getJsonSchema(doc[0]);
		if (items) {
			return {
				items,
			};
		}
	} else if (doc instanceof bson.BSONRegExp || doc instanceof RegExp) {
		return {
			type: 'regex'
		};
	} else if (doc instanceof bson.ObjectID) {
		return {
			type: 'objectId'
		}; 
	} else if (doc instanceof bson.MinKey) {
		return {
			type: 'minKey'
		}; 
	} else if (doc instanceof bson.MaxKey) {
		return {
			type: 'maxKey'
		}; 
	} else if (doc instanceof bson.Code) {
		return {
			type: 'JavaScript'
		};
	} else if (doc instanceof bson.Long) {
		return {
			type: 'numeric',
			mode: 'integer64'
		};
	} else if (typeof doc === "number" && Math.abs(doc) > 2**32) {
		return {
			type: 'numeric',
			mode: 'integer64'
		};
	} else if (doc instanceof bson.Decimal128) {
		return {
			type: 'numeric',
			mode: 'decimal128'
		};
	} else if (doc instanceof bson.Binary) {
		return {
			type: 'binary'
		}; 
	} else if (doc instanceof Date) {
		return {
			type: 'date'
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

function getBuildInfo(db) {
	return new Promise((resolve, reject) => {
		db.admin().buildInfo((err, info) => {
			if (err) {
				reject(err);
			} else {
				resolve(info);
			}
		});
	});
}

function getSamplingInfo(recordSamplingSettings, fieldInference){
	let samplingInfo = {};
	let value = recordSamplingSettings[recordSamplingSettings.active].value;
	let unit = (recordSamplingSettings.active === 'relative') ? '%' : ' records max';
	samplingInfo.recordSampling = `${recordSamplingSettings.active} ${value}${unit}`
	samplingInfo.fieldInference = (fieldInference.active === 'field') ? 'keep field order' : 'alphabetical order';
	return samplingInfo;
}

function handleBucket(_, async, connectionInfo, collectionNames, database, dbItemCallback){
	async.map(collectionNames, (collectionName, collItemCallback) => {
		const collection = database.collection(collectionName);
		if (!collection) {
			return collItemCallback(`Failed got collection ${collectionName}`);
		}

		getData(collection, connectionInfo.recordSamplingSettings, (err, documents) => {
			if(err){
				return collItemCallback(err);
			} else {
				documents  = filterDocuments(documents);
				let documentKind = connectionInfo.documentKinds[collectionName].documentKindName || '*';
				let documentTypes = [];

				if (documentKind !== '*') {
					documentTypes = documents.map(function(doc){
						return doc[documentKind];
					});
					documentTypes = documentTypes.filter((item) => Boolean(item));
					documentTypes = _.uniq(documentTypes);
				}

				let dataItem = prepareConnectionDataItem(_, documentTypes, collectionName, database, documents.length === 0);
				return collItemCallback(err, dataItem);
			}
		});
	}, (err, items) => {
		return dbItemCallback(createError(ERROR_HANDLE_BUCKET, err), items);
	});
}

function prepareConnectionDataItem(_, documentTypes, bucketName, database, isEmpty){
	let uniqueDocuments = _.uniq(documentTypes);
	let connectionDataItem = {
		dbName: bucketName,
		dbCollections: uniqueDocuments,
		isEmpty
	};

	return connectionDataItem;
}

function filterDocuments(documents){
	return documents.map(item =>{
		for(let prop in item) {
			if(prop && prop[0] === '_') {
				delete item[prop];
			}
		}
		return item;
	});
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
		? recordSamplingSettings.absolute.value
		: Math.round( count/100 * per);
}

function getData(collection, sampleSettings, callback) {
	collection.countDocuments((err, count) => {
		const amount = !err && count > 0 ? count : 1000;								
		const size = +getSampleDocSize(amount, sampleSettings) || 1000;
		const iterations = size > 1000 ? Math.ceil(size / 1000) : 1;

		asyncLoop(iterations, function (i, callback) {
			const limit = i < iterations - 1 || size % 1000 === 0 ? 1000 : size % 1000;
			
			collection.find().limit(limit).toArray(callback);									
		}, callback);
	});
}

function asyncLoop(iterations, callback, done) {
	let result = [];
	let i = 0; 
	let handler = function (err, data) {
		if (err) {
			done(err);
		} else {
			result = result.concat(data);
			
			if (++i < iterations) {
				callback(i, handler);
			} else {
				done(err, result);
			}
		}
	};

	try {
		callback(i, handler);
	} catch (e) {
		done(e);
	}
}

function createError(code, message) {
	if (!message) {
		return null;
	}
	if (message.code) {
		code = message.code;
	}
	message = message.message || message.msg || message.errmsg || message;

	return {code, message};
}

const createLogger = ({ title, logger, hiddenKeys }) => {
	return {
		info(message) {
			logger.log('info', { message }, title, hiddenKeys);
		},

		progress(message, dbName = '', tableName = '') {
			logger.progress({ message, containerName: dbName, entityName: tableName });
		},

		error(error) {
			logger.log('error', {
				message: error.message,
				stack: error.stack,
			}, title);
		}
	};
};
