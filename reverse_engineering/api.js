'use strict';

const CosmosClient = require('./CosmosClient');
const bson = require('bson');
const connectionHelper = require('./helpers/connectionHelper');

const ERROR_CONNECTION = 1;
const ERROR_DB_LIST = 2;
const ERROR_DB_CONNECTION = 3;
const ERROR_LIST_COLLECTION = 4;
const ERROR_GET_DATA = 5;
const ERROR_HANDLE_BUCKET = 6;
const ERROR_COLLECTION_DATA = 7;

module.exports = {
	connect: function(connectionInfo, logger, cb){
		logger.clear();
		logger.log('info', connectionInfo, 'Reverse-Engineering connection settings', connectionInfo.hiddenKeys);

		connectionHelper.connect(connectionInfo)
			.then(connection => cb(null, connection))
			.catch(err => {
				logger.log('error', createError(ERROR_CONNECTION, err), "Connection error");
				return cb({
					message: err.code === 18 ? 'Authentication failed. Please, check connection settings and try again' : err.message,
					stack: err.stack,
				});
			});
	},

	disconnect: function(connectionInfo, logger, cb){
		cb()
	},

	testConnection: function(connectionInfo, logger, cb){
		this.connect(connectionInfo, logger, (err, result) => {
			if (err) {
				cb(err);
			} else {
				cb(false);
				result.close();
			}
		});
	},

	getDatabases: function(connectionInfo, logger, cb){
		this.connect(connectionInfo, logger, (err, connection) => {
			if (err) {
				logger.log('error', err);
				return cb(err);
			} else {
				const db = connection.db();
				db.admin().listDatabases((err, dbs) => {
					if(err) {
						logger.log('error', err);
						connection.close();
						return cb(createError(ERROR_DB_LIST, err));
					} else {
						dbs = dbs.databases.map(item => item.name);
						logger.log('info', dbs, 'All databases list', connectionInfo.hiddenKeys);
						connection.close();
						return cb(null, dbs);
					}
				});
			}
		});
	},

	getDocumentKinds: function(connectionInfo, logger, cb, app) {
		const async = app.require('async');	
		this.connect(connectionInfo, logger, (err, connection) => {
			if (err) {
				logger.log('error', err);
				console.log(err);
			} else {
				const db = connection.db(connectionInfo.database);
				
				if (!db) {
					connection.close();
					return cb(createError(ERROR_DB_CONNECTION, `Failed connection to database ${connectionInfo.database}`));
				}

				db.listCollections().toArray((err, collections) => {
					if (err) {
						logger.log('error', err);
						connection.close();
						cb(createError(ERROR_LIST_COLLECTION, err));
					} else {
						collections = connectionInfo.includeSystemCollection ? collections : filterSystemCollections(collections);
						logger.log('info', collections, 'Mapped collection list');

						async.map(collections, (collectionData, collItemCallback) => {
							const collection = db.collection(collectionData.name);

							getData(collection, connectionInfo.recordSamplingSettings, function (err, documents) {
								if (err) {
									logger.log('error', err);
									return collItemCallback(err, null);
								} else {
									logger.log('info', { collectionItem: collectionData.name }, 'Getting documents for current collectionItem', connectionInfo.hiddenKeys);
									
									documents  = filterDocuments(documents);
									let inferSchema = generateCustomInferSchema(documents, { sampleSize: 20 });
									let documentsPackage = getDocumentKindDataFromInfer({ 
										bucketName: collectionData.name,
										inference: inferSchema, 
										isCustomInfer: true, 
										excludeDocKind: connectionInfo.excludeDocKind 
									}, 90);

									return collItemCallback(err, documentsPackage);
								}
							});
						}, (err, items) => {
							if(err){
								logger.log('error', err);
							}

							connection.close();		
							return cb(createError(ERROR_GET_DATA, err), items);
						});
					}
				});
			}			
		});
	},

	getDbCollectionsNames: function(connectionInfo, logger, cb, app) {
		const _ = app.require('lodash');
		const async = app.require('async');
		this.connect(connectionInfo, logger, (err, connection) => {
			if (err) {
				logger.log('error', err);
				return cb(err)
			} else {
				const db = connection.db(connectionInfo.database);
				
				if (!db) {
					connection.close();
					return cb(createError(ERROR_DB_CONNECTION, `Failed connection to database ${connectionInfo.database}`));
				}

				logger.log('info', { Database: connectionInfo.database }, 'Getting collections list for current database', connectionInfo.hiddenKeys);
				
				db.listCollections().toArray((err, collections) => {
					if(err){
						logger.log('error', err);
						connection.close();
						return cb(createError(ERROR_LIST_COLLECTION, err));
					} else {
						let collectionNames = (connectionInfo.includeSystemCollection ? collections : filterSystemCollections(collections)).map(item => item.name);
						logger.log('info', collectionNames, "Collection list for current database", connectionInfo.hiddenKeys);
						handleBucket(_, async, connectionInfo, collectionNames, db, function (err, items) {
							connection.close();
							if (err) {
								cb(err);
							} else {
								cb(null, items);
							}
						});
					}
				});
			}
		});
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

function generateCustomInferSchema(documents, params) {
	function typeOf(obj) {
		return {}.toString.call(obj).split(' ')[1].slice(0, -1).toLowerCase();
	};

	let sampleSize = params.sampleSize || 30;

	let inferSchema = {
		"#docs": 0,
		"$schema": "http://json-schema.org/schema#",
		"properties": {}
	};

	documents.forEach(item => {
		inferSchema["#docs"]++;
		
		for(let prop in item){
			if(inferSchema.properties.hasOwnProperty(prop)){
				inferSchema.properties[prop]["#docs"]++;
				inferSchema.properties[prop]["samples"].indexOf(item[prop]) === -1 && inferSchema.properties[prop]["samples"].length < sampleSize? inferSchema.properties[prop]["samples"].push(item[prop]) : '';
				inferSchema.properties[prop]["type"] = typeOf(item[prop]);
			} else {
				inferSchema.properties[prop] = {
					"#docs": 1,
					"%docs": 100,
					"samples": [item[prop]],
					"type": typeOf(item[prop])
				}
			}
		}
	});

	for (let prop in inferSchema.properties){
		inferSchema.properties[prop]["%docs"] = Math.round((inferSchema.properties[prop]["#docs"] / inferSchema["#docs"] * 100), 2);
	}
	return inferSchema;
}

function getDocumentKindDataFromInfer(data, probability){
	let suggestedDocKinds = [];
	let otherDocKinds = [];
	let documentKind = {
		key: '',
		probability: 0	
	};

	if(data.isCustomInfer){
		let minCount = Infinity;
		let inference = data.inference.properties;

		for(let key in inference){
			if (typeof inference[key].samples[0] === 'object') {
				continue;
			}

			if(inference[key]["%docs"] >= probability && inference[key].samples.length){
				suggestedDocKinds.push(key);

				if(data.excludeDocKind.indexOf(key) === -1){
					if (inference[key]["%docs"] === documentKind.probability && documentKind.key === 'type') {
						continue;
					}
					
					if(inference[key]["%docs"] >= documentKind.probability && inference[key].samples.length < minCount){
						minCount = inference[key].samples.length;
						documentKind.probability = inference[key]["%docs"];
						documentKind.key = key;
					}
				}
			} else {
				otherDocKinds.push(key);
			}
		}
	} else {
		let flavor = (data.flavorValue) ? data.flavorValue.split(',') : data.inference[0].Flavor.split(',');
		if(flavor.length === 1){
			suggestedDocKinds = Object.keys(data.inference[0].properties);
			let matсhedDocKind = flavor[0].match(/([\s\S]*?) \= "?([\s\S]*?)"?$/);
			documentKind.key = (matсhedDocKind.length) ? matсhedDocKind[1] : '';
		}
	}

	let documentKindData = {
		bucketName: data.bucketName,
		documentList: suggestedDocKinds,
		documentKind: documentKind.key,
		preSelectedDocumentKind: data.preSelectedDocumentKind,
		otherDocKinds
	};

	return documentKindData;
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

const getShardingKey = (db, collectionName) => new Promise((resolve, reject) => {
	db.command({
		customAction: 'GetCollection',
		collection: collectionName
	}, (err, result) => {
		if (err) {
			return reject(err);
		}

		if (!result.shardKeyDefinition) {
			return resolve('');
		}

		const shardingKey = Object.keys(result.shardKeyDefinition)[0] || '';

		return resolve(shardingKey);
	});
});

const getAllTypesIndexes = (db, collectionName, shardingKey) => new Promise((resolve, reject) => {
	db.command({
		listIndexes: collectionName
	}, (err, result) => {
		if (err) {
			return reject(err);
		}

		const allIndexes = result?.cursor?.firstBatch || [];

		const uniqueKeys = allIndexes.filter(item => {
			return item.unique;
		}).map((item) => {
			return {
				attributePath: Object.keys(item.key).filter(key => key !== shardingKey)
			};
		});

		const ttlIndex = allIndexes.filter(item => {
			return item.expireAfterSeconds !== undefined;
		}).map((item) => {
			return {
				expireAfterSeconds: item.expireAfterSeconds,
				name: item.name,
				key:  Object.keys(item.key)[0],
			};
		})[0];

		const indexes = allIndexes.filter(index => {
			if (index.unique) {
				return false;
			}

			if (index.expireAfterSeconds !== undefined) {
				return false;
			}

			if (Object.keys(index.key).some(key => key === 'DocumentDBDefaultIndex')) {
				return false;
			}

			return true;
		}).map(index => {
			const getIndexType = (index) => {
				const isCompound = Object.values(index.key).length > 1;
				const isWildcard = Object.keys(index.key).some(key => key.endsWith('$**'));

				if (isCompound) {
					return 'Compound';
				} else if (isWildcard) {
					return 'Wildcard';
				} else {
					return 'Single Field';
				}
			};
			const type = (indexType) => {
				if (indexType === -1) {
					return 'descending';
				} else if (indexType === '2dsphere') {
					return '2dsphere';
				} else {
					return 'ascending';
				}
			};

			return {
				name: index.name,
				indexType: getIndexType(index),
				indexKey: Object.keys(index.key).map(key => ({
					type: type(index.key[key]),
					name: key.replace(/\.\$\*\*$/, ''),
				}))
			};
		});

		resolve({ uniqueKeys, ttlIndex, indexes });
	});
});

function getBucketInfo(dbInstance, collectionName, logError, cb) {
	let bucketInfo = {};

	getShardingKey(dbInstance, collectionName)
	.then(result => {
		bucketInfo.shardKey = result;

		return getAllTypesIndexes(dbInstance, collectionName, result);
	}, logError)
	.then(result => {
		bucketInfo.uniqueKey = result.uniqueKeys;
		bucketInfo.indexes = result.indexes;

		bucketInfo = {
			...bucketInfo,
			...convertTtlIndex(result.ttlIndex),
		};
	}, logError)
	.then(() => {
		cb(null, bucketInfo);
	}, err => {
		cb(err, bucketInfo);
	});

}

function convertTtlIndex(ttlIndex) {
	let TTL = 'Off';

	if (!ttlIndex) {
		return { TTL };
	}

	if (ttlIndex.expireAfterSeconds === -1) {
		TTL = 'On (no default)';
	} else {
		TTL = 'On';
	}

	const TTLseconds = ttlIndex.expireAfterSeconds;

	return {
		TTL,
		TTLseconds,
	};
}
