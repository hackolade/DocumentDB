let instance;

const awsHelper = (connectionInfo, awsSdk) => {
	if (instance) {
		return instance;
	}
	
	let awsOptions = ['accessKeyId', 'secretAccessKey', 'sessionToken', 'region'].reduce((options, key) => {
		if (!connectionInfo[key]) {
			return options;
		}

		return {
			...options,
			[key]: connectionInfo[key],
		};
	}, {});

	awsSdk.config.update(awsOptions);
	const docDb = new awsSdk.DocDB();

	const dbClusterIdentifier = connectionInfo.dbClusterIdentifier;
	const clusterRegion = connectionInfo.region;

	instance =  {
		getCluster() {
			return new Promise((resolve, reject) => {
				docDb.describeDBClusters({
					DBClusterIdentifier: dbClusterIdentifier,
				}, (err, data) => {
					if (err) {
						reject(err);
					} else {
						resolve(data?.['DBClusters']?.[0]);
					}
				});
			});
		},
		getRegion() {
			return clusterRegion;
		},
		tags(resourceName) {
			return new Promise((resolve, reject) => {
				docDb.listTagsForResource({
					ResourceName: resourceName,
				}, (err, data) => {
					if (err) {
						reject(err);
					} else {
						resolve(data);
					}
				});
			}); 
		},
	};

	return instance;
};

module.exports = awsHelper;
