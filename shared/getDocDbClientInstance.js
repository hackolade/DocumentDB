const { head } = require('lodash');
const { DocDBClient, DescribeDBClustersCommand, ListTagsForResourceCommand } = require('@aws-sdk/client-docdb');
const { hckFetchAwsSdkHttpHandler } = require('@hackolade/fetch');

let instance = null;

const getDocDbClientInstance = ({ connectionInfo = {} } = {}) => {
	if (instance) {
		return instance;
	}

	const { region, accessKeyId, secretAccessKey, sessionToken, queryRequestTimeout, dbClusterIdentifier } =
		connectionInfo;
	const docDbClient = new DocDBClient({
		region,
		credentials: {
			accessKeyId,
			secretAccessKey,
			sessionToken,
		},
		requestHandler: hckFetchAwsSdkHttpHandler({ requestTimeout: queryRequestTimeout }),
	});

	instance = {
		getCluster() {
			const result = docDbClient.send(
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
