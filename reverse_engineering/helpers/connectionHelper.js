const MongoClient = require('mongodb').MongoClient;
const ssh = require('tunnel-ssh');
const fs = require('fs');

let sshTunnel;
let connection;

function getSshConfig(info) {
	const config = {
		username: info.ssh_user || 'ec2-user',
		host: info.ssh_host,
		port: info.ssh_port || 22,
		dstHost: info.host,
		dstPort: info.port,
		localHost: '127.0.0.1',
		localPort: info.port,
		keepAlive: true,
	};

	return Object.assign({}, config, {
		privateKey: fs.readFileSync(info.ssh_key_file),
		passphrase: info.ssh_key_passphrase
	});
};

const connectViaSsh = (info) => new Promise((resolve, reject) => {
	ssh(getSshConfig(info), (err, tunnel) => {
		if (err) {
			reject(err);
		} else {
			resolve({
				tunnel,
				info: Object.assign({}, info, {
					host: '127.0.0.1',
				})
			});
		}
	});
});

function generateConnectionParams(connectionInfo){
	if (connectionInfo.sslType === 'TRUST_CUSTOM_CA_SIGNED_CERTIFICATES' && connectionInfo.ssh) {
		return {
			url: `mongodb://${connectionInfo.username}:${connectionInfo.password}@${connectionInfo.host}:${connectionInfo.port}/`,
			options: {
				tls: true,
				tlsCAFile: connectionInfo.certAuthority,
				tlsAllowInvalidHostnames: true,
				useUnifiedTopology: true,
				sslValidate: false,
			}
		};
	}
	if (connectionInfo.sslType === 'TRUST_CUSTOM_CA_SIGNED_CERTIFICATES') {
		return {
			url: `mongodb://${connectionInfo.username}:${connectionInfo.password}@${connectionInfo.host}:${connectionInfo.port}/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`,
			options: {
				tlsCAFile: connectionInfo.certAuthority,
				useUnifiedTopology: true,
			}
		};
	} else {
		return {
			url: `mongodb://${connectionInfo.username}:${connectionInfo.password}@${connectionInfo.host}:${connectionInfo.port}/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`,
			options: {
				useNewUrlParser: true,
				useUnifiedTopology: true,
			}
		};
	}
}

async function connect(connectionInfo) {
	try {
		if (connectionInfo.ssh) {
			const {tunnel, info} = await connectViaSsh(connectionInfo);
			sshTunnel = tunnel;
			connectionInfo = info;
		}
	
		const params = generateConnectionParams(connectionInfo);
	
		connection = await MongoClient.connect(params.url, params.options);

		return createConnection(connection);
	} catch (err) {
		throw {
			message: err.code === 18 ? 'Authentication failed. Please, check connection settings and try again' : err.message,
			stack: err.stack,
		};
	}
}

function createConnection(connection) {
	return {
		getDatabases() {
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
		},
		getCollections(dbName) {
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
		}
	};
}

function close() {
	if (connection) {
		connection.close();
		connection = null;
	}

	if (sshTunnel) {
		sshTunnel.close();
		sshTunnel = null;
	}
};

module.exports = {
	connect,
	close,
};
