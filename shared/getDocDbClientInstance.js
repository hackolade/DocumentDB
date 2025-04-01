const { head } = require('lodash');
const { fromIni } = require('@aws-sdk/credential-providers');
const { DocDBClient, DescribeDBClustersCommand, ListTagsForResourceCommand } = require('@aws-sdk/client-docdb');
const { hckFetchAwsSdkHttpHandler } = require('@hackolade/fetch');

let instance = null;

const getCredentials = async ({ connectionInfo = {}, logger = {} }) => {
	const { accessKeyId, secretAccessKey, sessionToken } = connectionInfo;

	if (!accessKeyId || !secretAccessKey) {
		logger.info(`'Access Key ID' or 'Secret Access Key' were not specified, checking system AWS credentials...`);
		try {
			return await fromIni()();
		} catch (error) {
			logger.error(error);
			return {};
		}
	}

	if (sessionToken) {
		logger.info(`AWS session token provided, including it into credentials.`);
	}

	return {
		accessKeyId,
		secretAccessKey,
		sessionToken,
	};
};

const getDocDbClientInstance = async ({ connectionInfo = {}, logger } = {}) => {
	if (instance) {
		return instance;
	}

	const { region, queryRequestTimeout, dbClusterIdentifier } = connectionInfo;

	const docDbClient = new DocDBClient({
		region,
		credentials: await getCredentials({ connectionInfo, logger }),
		requestHandler: hckFetchAwsSdkHttpHandler({ requestTimeout: queryRequestTimeout }),
	});

	instance = {
		async getCluster() {
			const result = await docDbClient.send(
				new DescribeDBClustersCommand({
					DBClusterIdentifier: dbClusterIdentifier,
				}),
			);
			return head(result.DBClusters);
		},
		getRegion() {
			return connectionInfo.region;
		},
		tags(resourceName) {
			return docDbClient.send(
				new ListTagsForResourceCommand({
					ResourceName: resourceName,
				}),
			);
		},
	};

	return instance;
};

module.exports = { getDocDbClientInstance };
