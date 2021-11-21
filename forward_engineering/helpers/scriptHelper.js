
const isObjectEmpty = obj => Object.keys(obj).length === 0;

const filterObject = obj => Object.fromEntries(Object.entries(obj).filter(([key, value]) => value !== undefined));

const createIndexStatement = (...args) => {
	return 'createIndex(' + args.map(filterObject).filter(arg => !isObjectEmpty(arg)).map(stringify).join(', ') + ');';
};

const stringify = (data) => JSON.stringify(data, null, 2);

const getIndexType = (indexType) => {
	return ({
		'descending': -1,
		'ascending': 1,
		'2dsphere': '2dsphere',
	})[indexType] || 1;
};

const createIndex = (index) => {
	let indexKeys = index?.indexKey;

	if (!Array.isArray(indexKeys)) {
		return '';
	}

	indexKeys = indexKeys.filter(index => index.name);

	if (indexKeys.length === 0) {
		return '';
	}

	return createIndexStatement(
		indexKeys.reduce((result, indexKey) => ({
			...result,
			[indexKey.name]: getIndexType(indexKey.type),
		}), {}),
		{
			name: index.name,
		}
	);
};

const createTtlIndex = (containerData = {}) => {
	if (containerData.TTL === 'Off' || !containerData.TTL) {
		return '';
	}

	return createIndexStatement({
		_ts: 1,
	}, filterObject({
		name: 'ttl',
		expireAfterSeconds: containerData.TTL === 'On (no default)'
			? -1
			: containerData.TTLseconds,
	}));
};

const createUniqueIndex = (uniqueKeys, shardKey) => {
	if (!Array.isArray(uniqueKeys)) {
		return '';
	}

	uniqueKeys = uniqueKeys.filter(index => index.name);

	if (uniqueKeys.length === 0) {
		return '';
	}

	return createIndexStatement(
		uniqueKeys.reduce((result, indexKey) => ({
			...result,
			[indexKey.name]: 1,
		}), shardKey ? {
			[shardKey]: 1,
		} : {}),
		{
			unique: true,
		}
	);
};

const getContainerName = (containerData) => {
	return containerData[0]?.code || containerData[0]?.name;
};

const getCollection = (name) => {
	return `db.getCollection("${name}")`;
};

const getIndexes = (containerData) => {
	const indexes = containerData[1]?.indexes || [];
	const uniqueIndexes = containerData[0]?.uniqueKey || [];
	const shardKey = containerData[0]?.shardKey?.[0]?.name;

	return [
		...uniqueIndexes.map(uniqueKey => createUniqueIndex(uniqueKey.attributePath, shardKey)),
		...indexes.filter(index => index.isActivated !== false).map(createIndex),
		createTtlIndex(containerData[0]),
	].filter(Boolean).map(index => getCollection(getContainerName(containerData)) + '.' + index).join('\n\n');
};

const createShardKey = ({ containerData }) => {
	const shardKey = containerData[0]?.shardKey?.[0]?.name;

	if (!shardKey) {
		return '';
	}

	const dbId = getDbId(containerData);
	const name = getContainerName(containerData);

	return `use admin;\ndb.runCommand({ shardCollection: "${dbId}.${name}", key: { "${shardKey}": "hashed" }});`
};

const getDbId = (data) => {
	return data[0]?.dbId;
};

const getScript = (data) => {
	const name = getDbId(data.containerData);
	const useDb = name ? `use ${name};` : '';
	const indexes = getIndexes(data.containerData);

	return [createShardKey(data), useDb, indexes].filter(Boolean).join('\n\n');
};

const updateSample = (sample, containerData, entityData) => {		
	const docType = containerData?.docTypeName;

	if (!docType) {
		return sample;
	}

	let data = JSON.parse(encodedExtendedTypes(sample));

	return decodedExtendedTypes(JSON.stringify({
		...data,
		[docType]: entityData.code || entityData.collectionName,
	}, null, 2));
};

const insertSample = ({ containerData, entityData, sample }) => {
	return getCollection(getContainerName(containerData)) + `.insert(${updateSample(sample, containerData[0], entityData?.[0] || {})});`;
};

const insertSamples = (data) => {
	const name = getDbId(data.containerData);
	const useDb = name ? `use ${name};` : '';
	const samples = data.entities.map(entityId => insertSample({
		containerData: data.containerData,
		entityData: (data.entityData[entityId] || []),
		sample: data.jsonData[entityId],
	})).join('\n\n');

	return [useDb, samples].filter(Boolean).join('\n\n');
};


function decodedExtendedTypes(data, returnInStr) {
	let decodedData;
	let strData = data;
	let lineBeginning = '';
	let lineEnding = '';

	decodedData = strData
		.replace(/\"\$__oid_(.*?)\"/gi, function (a, b) {
			return lineBeginning + 'ObjectId("' + b + '")' + lineEnding;
		})
		.replace(/\"\$__date_(.*?)\"/gi, function (a, b) {
			return lineBeginning + 'ISODate("' + b + '")' + lineEnding;
		})
		.replace(/\"(?:CURRENT_)?\$__tmstmp_(.*?)\"/gi, function (a, b) {
			return lineBeginning + 'Timestamp(' + b + ')' + lineEnding;
		})
		.replace(/\"\$__rgxp_(.*?)\"/gi, function (a, b) {
			return lineBeginning + b + lineEnding;
		})
		.replace(/\"\$__bindata_(\d*)_(.*?)\"/gi, function (a, b, c) {
			return lineBeginning + `BinData(${b},"${c}")` + lineEnding;
		})
		.replace(/\"\$__maxKey_(\d*)\"/gi, function (a, b) {
			return lineBeginning + `new MaxKey(${b})` + lineEnding;
		})
		.replace(/\"\$__minKey_(\d*)\"/gi, function (a, b) {
			return lineBeginning + `new MinKey(${b})` + lineEnding;
		})
		.replace(/\"\$__jswscope_(.*?})\"/gi, function (a, b) {
			return lineBeginning + `Code("${b}")` + lineEnding;
		})
		.replace(/\"\$__js_(.*?})\"/gi, function (a, b) {
			return lineBeginning + `Code("${b}")` + lineEnding;
		});

	return decodedData;
};

function encodedExtendedTypes(data, returnInStr) {
	let encodedData;
	let lineBeginning = '"';
	let lineEnding = '"';

	encodedData = data
		.replace(/ObjectId\(\"(.*?)\"\)/gi, function (a, b) {
			return lineBeginning + '$__oid_' + b + lineEnding;
		})
		.replace(/ISODate\(\"(.*?)\"\)/gi, function (a, b) {
			return lineBeginning + '$__date_' + b + lineEnding;
		})
		.replace(/Timestamp\((.*?)\)/gi, function (a, b) {
			return lineBeginning + '$__tmstmp_' + b + lineEnding;
		})
		.replace(/\"\s*:\s*\/(.*?)\/([^,\s\]\}\n]*)/gi, function (a, b, c) {
			return '": ' + lineBeginning + '$__rgxp_/' + b + '/' + c + lineEnding;
		})
		.replace(/BinData\((\d*),\"(.*?)\"\)/gi, function (a, b, c) {
			return lineBeginning + `$__bindata_${b}_${c}` + lineEnding;
		})
		.replace(
			/\"type\": \"JavaScript\(w\/scope\)\",\s*.*?\"sample\": \{\s*\"_bsontype\": \"Code\",\s*\"code\": \"(.*?})\",\s*\"scope\": .*?\s*\}/gi,
			function (a, b) {
				return a.replace(
					/\{\s*\"_bsontype\": \"Code\",\s*\"code\": \"(.*?})\",\s*\"scope\": .*?\s*\}/,
					function (a, b) {
						return lineBeginning + `$__jswscope_${b}` + lineEnding;
					},
				);
			},
		)
		.replace(
			/\{\s*\"_bsontype\": \"Code\",\s*\"code\": \"(.*?})\".*?\s*\}/gi,
			function (a, b) {
				return lineBeginning + `$__js_${b}` + lineEnding;
			},
		)
		.replace(/\"type\": \"minKey\",\s*.*?\"sample\": \{\s*\"\$minKey\": (\d*)\s*\}/gi, function (a, b) {
			return a.replace(/\{\s*\"\$minKey\": (\d*)\s*\}/, function (a, b) {
				return lineBeginning + `$__minKey_${b}` + lineEnding;
			});
		})
		.replace(/\"type\": \"maxKey\",\s*.*?\"sample\": \{\s*\"\$maxKey\": (\d*)\s*\}/gi, function (a, b) {
			return a.replace(/\{\s*\"\$maxKey\": (\d*)\s*\}/, function (a, b) {
				return lineBeginning + `$__maxKey_${b}` + lineEnding;
			});
		});

	return encodedData;
};

module.exports = {
	getScript,
	insertSample,
	insertSamples,
};
