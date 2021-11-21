const MongoClient = require('mongodb').MongoClient;

function generateConnectionParams(connectionInfo){
	return {
		url: `mongodb://${connectionInfo.userName}:${connectionInfo.password}@${connectionInfo.host}:${connectionInfo.port}?ssl=true`,
		options: {
			sslValidate:false,
			useNewUrlParser: true,
			useUnifiedTopology: true,
		}
	};
}

function connect(connectionInfo) {
	const params = generateConnectionParams(connectionInfo);

	return MongoClient.connect(params.url, params.options);
}

module.exports = {
	connect,
};
