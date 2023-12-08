const MongoClient = require('mongodb').MongoClient;
const mongodbSample = require('mongodb-collection-sample');

let sshTunnel;
let connection;

function getSshConfig(info) {
	return {
		sshTunnelUsername: info.ssh_user || 'ec2-user',
		sshTunnelHostname: info.ssh_host,
		sshTunnelPort: Number(info.ssh_port) || 22,
		sshAuthMethod: info.ssh_method === 'privateKey' ? 'IDENTITY_FILE' : '',
		sshTunnelIdentityFile: info.ssh_key_file,
		sshTunnelPassphrase: info.ssh_key_passphrase,
		host: info.host,
		port: Number(info.port),
	};
}

const getSshConnectionSettings = async (connectionInfo, sshService) => {
	const sshConnectionConfig = getSshConfig(connectionInfo);
	const { options } = await sshService.openTunnel(sshConnectionConfig);
	return {
		...connectionInfo,
		host: options.host,
		port: options.port.toString() || '22',
	};
};

function generateConnectionParams(connectionInfo) {
	const username = encodeURIComponentRFC3986(connectionInfo.username);
	const password = encodeURIComponentRFC3986(connectionInfo.password);

	if (
		(connectionInfo.sslType === 'TRUST_CUSTOM_CA_SIGNED_CERTIFICATES' && connectionInfo.ssh) ||
		connectionInfo.sslType === 'UNVALIDATED_SSL'
	) {
		return {
			url: `mongodb://${username}:${password}@${connectionInfo.host}:${connectionInfo.port}/?retryWrites=false`,
			options: {
				tls: true,
				tlsAllowInvalidHostnames: true,
				useUnifiedTopology: true,
				sslValidate: false,
			},
		};
	}
	if (connectionInfo.sslType === 'TRUST_CUSTOM_CA_SIGNED_CERTIFICATES') {
		return {
			url: `mongodb://${username}:${password}@${connectionInfo.host}:${connectionInfo.port}/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`,
			options: {
				tlsCAFile: connectionInfo.certAuthority,
				useUnifiedTopology: true,
			},
		};
	} else {
		return {
			url: `mongodb://${username}:${password}@${connectionInfo.host}:${connectionInfo.port}/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`,
			options: {
				useNewUrlParser: true,
				useUnifiedTopology: true,
			},
		};
	}
}

async function connect(connectionInfo, sshService) {
	try {
		if (connection) {
			return createConnection(connection);
		}

		if (connectionInfo.ssh) {
			connectionInfo = await getSshConnectionSettings(connectionInfo, sshService);
			sshTunnel = true;
		}

		const params = generateConnectionParams(connectionInfo);

		connection = await MongoClient.connect(params.url, params.options);

		return createConnection(connection);
	} catch (err) {
		throw {
			message:
				err.code === 18
					? 'Authentication failed. Please, check connection settings and try again'
					: err.message,
			stack: err.stack,
		};
	}
}

function createConnection(connection) {
	const getDatabases = () => {
		return new Promise((resolve, reject) => {
			const db = connection.db();
			db.admin().listDatabases((err, dbs) => {
				if (err) {
					return reject(err);
				} else {
					return resolve(dbs.databases);
				}
			});
		});
	};
	const getCollections = dbName => {
		return new Promise((resolve, reject) => {
			const db = connection.db(dbName);

			if (!db) {
				return reject(new Error(`Failed connection to database "${dbName}"`));
			}

			db.listCollections().toArray((err, collections) => {
				if (err) {
					return reject(err);
				} else {
					return resolve(collections);
				}
			});
		});
	};

	const getBuildInfo = () => {
		return new Promise((resolve, reject) => {
			const db = connection.db();

			db.admin().buildInfo((err, info) => {
				if (err) {
					reject(err);
				} else {
					resolve(info);
				}
			});
		});
	};

	const getDataStream = (dbName, collectionName, options) => {
		const db = connection.db(dbName);

		if (!options.sort || Object.keys(options.sort).length === 0) {
			return mongodbSample(db, collectionName, options);
		}

		const collection = db.collection(collectionName);

		return collection
			.find(options.query, {
				sort: options.sort,
				limit: Number(options.size),
				maxTimeMS: options.maxTimeMS,
			})
			.stream();
	};

	const getRandomDocuments = (dbName, collectionName, { query, sort, limit, maxTimeMS }) => {
		return new Promise((resolve, reject) => {
			const RANDOM_SAMPLING_ERROR_CODE = 28799;
			const INTERRUPTED_OPERATION = 11601;
			let sampledDocs = [];
			const options = {
				size: Number(limit),
				query: query,
				sort: sort,
				maxTimeMS: maxTimeMS || 120000,
			};
			let streamError;

			const streamErrorHandler = err => {
				if (err.code === RANDOM_SAMPLING_ERROR_CODE) {
					err = {
						message:
							'MongoDB Error: $sample stage could not find a non-duplicate document after 100 while using a random cursor. Please try again.',
					};
				}

				if (err.code === INTERRUPTED_OPERATION) {
					const newError = new Error(
						'MongoDB Error: ' +
							err.message +
							'. Please, try to increase query timeout (Options -> Reverse-Engineering) and try again.',
					);
					newError.stack = err.stack;
					err = newError;
				}

				streamError = err;

				reject(err);
			};

			const stream = getDataStream(dbName, collectionName, options);
			stream.on('error', err => {
				streamErrorHandler(err);
			});

			stream.on('data', doc => {
				sampledDocs.push(doc);
			});

			stream.on('end', () => {
				if (streamError) {
					return;
				}

				resolve(sampledDocs);
			});
		});
	};

	const getCount = (dbName, collectionName) => {
		return new Promise((resolve, reject) => {
			const db = connection.db(dbName);
			const collection = db.collection(collectionName);

			collection.estimatedDocumentCount((err, count) => {
				if (err) {
					return reject(err);
				} else {
					return resolve(count);
				}
			});
		});
	};

	const findOne = (dbName, collectionName, query) => {
		return new Promise((resolve, reject) => {
			const db = connection.db(dbName);
			const collection = db.collection(collectionName);

			collection.findOne(query, (err, result) => {
				if (err) {
					return reject(err);
				} else {
					return resolve(result);
				}
			});
		});
	};

	const hasPermission = async (dbName, collectionName) => {
		try {
			await findOne(dbName, collectionName);

			return true;
		} catch (e) {
			if (e.code === 13) {
				return false;
			}

			throw e;
		}
	};

	const getIndexes = (dbName, collectionName) => {
		return new Promise((resolve, reject) => {
			const db = connection.db(dbName);
			const collection = db.collection(collectionName);

			collection.indexes({ full: true }, (err, indexes) => {
				if (err) {
					return reject(err);
				}

				return resolve(indexes);
			});
		});
	};

	const getCollection = (dbName, collectionName) => {
		const db = connection.db(dbName);
		const collection = db.collection(collectionName);

		return {
			createIndex(fields, options) {
				return collection.createIndex(fields, options);
			},
			insertOne(data) {
				return collection.insertOne(data);
			},
		};
	};

	const createCollection = async (dbName, collectionName) => {
		const COLLECTION_ALREADY_EXISTS_ERROR = 48;
		const db = connection.db(dbName);

		try {
			return await db.createCollection(collectionName);
		} catch (error) {
			if (error.code === COLLECTION_ALREADY_EXISTS_ERROR) {
				return;
			}
			throw error;
		}
	};

	return {
		getDatabases,
		getCollections,
		getBuildInfo,
		getDataStream,
		getCount,
		getRandomDocuments,
		findOne,
		hasPermission,
		getIndexes,
		getCollection,
		createCollection,
	};
}

async function close(sshService) {
	if (connection) {
		connection.close();
		connection = null;
	}

	if (sshTunnel) {
		sshTunnel = false;
		await sshService.closeConsumer();
	}
}

const encodeURIComponentRFC3986 = str => {
	return encodeURIComponent(str).replace(/[!'()*]/g, function (c) {
		return '%' + c.charCodeAt(0).toString(16);
	});
};

module.exports = {
	connect,
	close,
};
