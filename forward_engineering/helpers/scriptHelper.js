
const schemaHelper = require('./schemaHelper');

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
		'2DSphere': '2dsphere',
	})[indexType] || 1;
};

const createIndex = (index) => {
	let indexKeys = index?.key;

	if (!Array.isArray(indexKeys)) {
		return '';
	}

	indexKeys = indexKeys.filter(index => index.name && index.isActivated);

	if (indexKeys.length === 0) {
		return '';
	}

	return createIndexStatement(
		indexKeys.reduce((result, indexKey) => ({
			...result,
			[indexKey.name]: getIndexType(indexKey.type),
		}), {}),
		filterObject({
			name: index.name,
			unique: index.unique ? index.unique : undefined,
			sparse: index.sparse ? index.sparse : undefined,
			background: index.background ? index.background : undefined,
			expireAfterSeconds: index.expireAfterSeconds ? index.expireAfterSeconds : undefined,
		})
	);
};

const getContainerName = (containerData) => {
	return containerData[0]?.code || containerData[0]?.name;
};

const getCollectionName = (entityData) => {
	return entityData[0]?.code || entityData[0]?.collectionName;
};

const getCollection = (name) => {
	return `db.getCollection("${name}")`;
};

const getIndexes = (entityData, data) => {
	const indexes = entityData[1]?.Indxs || [];

	return indexes
		.filter(index => index.isActivated !== false)
		.map(fillKeys(data))
		.map(createIndex)
		.filter(Boolean)
		.map(index => getCollection(getCollectionName(entityData)) + '.' + index)
		.join('\n\n');
};

const createCollection = (entityData) => {
	return `db.createCollection("${getCollectionName(entityData)}");`;
};

const getScript = (data) => {
	const indexes = getIndexes(data.entityData, data);
	const createStatement = createCollection(data.entityData);

	return [createStatement, indexes].filter(Boolean).join('\n\n');
};

const updateSample = (sample) => {		
	let data = JSON.parse(encodedExtendedTypes(sample));

	return decodedExtendedTypes(JSON.stringify({
		...data,
	}, null, 2));
};

const fillKeys = ({ jsonSchema, definitions }) => (index) => {
	const hashTable = schemaHelper.getNamesByIds(
		index.key.map(key => key.keyId),
		[
			jsonSchema,
			definitions.internal,
			definitions.model,
			definitions.external,
		]
	);

	return {
		...index,
		key: index.key.map(key => {
			return {
				...key,
				...(hashTable[key.keyId] || {})
			};
		}),
	};
};

const insertSample = ({ entityData, sample }) => {
	return getCollection(getCollectionName(entityData)) + `.insert(${updateSample(sample)});`;
};

const insertSamples = (data) => {
	const useDb = useDbStatement(data.containerData);
	const samples = data.entities.map(entityId => insertSample({
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
		.replace(/\{\s*\"\$minKey\": (\d*)\s*\}/, function (a, b) {
			return lineBeginning + `$__minKey_${b}` + lineEnding;
		})
		.replace(/\{\s*\"\$maxKey\": (\d*)\s*\}/, function (a, b) {
			return lineBeginning + `$__maxKey_${b}` + lineEnding;
		});

	return encodedData;
};

const useDbStatement = (containerData) => {
	const name = getContainerName(containerData);
	const useDb = name ? `use ${name};` : '';

	return useDb;
};

module.exports = {
	getScript,
	insertSample,
	insertSamples,
	useDbStatement,
};
