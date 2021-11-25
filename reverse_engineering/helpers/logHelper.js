const os = require('os');
const packageFile = require('../../package.json');

const getPluginVersion = () => packageFile.version;

const getSystemInfo = (appVersion) => {
	return '' + 
	`Date: ${new Date()}` + '\n' + 
	`Application version: ${appVersion}` + '\n' + 
	`Plugin version: ${getPluginVersion()}` + '\n\n' + 
	`System information:` + '\n' +
	` Hostname:  ${os.hostname()}` + '\n' +
	` Platform:  ${os.platform()} ${os.arch()}` + '\n' +
	` Release:   ${os.release()}` + '\n' +
	` Uptime:    ${toTime(os.uptime())}` + '\n' +
	` Total RAM: ${(os.totalmem()/1073741824).toFixed(2)} GB` + '\n' +
	` CPU Model: ${os.cpus()[0].model}` + '\n' +
	` CPU Clock: ${maxClock(os.cpus())} MHZ` + '\n' +
	` CPU Cores: ${os.cpus().length} cores` + '\n\n';
};


const maxClock = (cpus) => {
	return cpus.reduce((highestClock, cpu) => Math.max(highestClock, cpu.speed), 0);
};

const prefixZero = (number) => number < 10 ? "0" + number : number;

const toTime = (number) => {
	return (Math.floor(number/3600))+":"+prefixZero(parseInt((number/3600-Math.floor(number/3600))*60))
};

const createLogger = ({ title, logger, hiddenKeys }) => {
	return {
		info(message) {
			logger.log('info', { message }, title, hiddenKeys);
		},

		progress(message, dbName = '', tableName = '') {
			logger.progress({ message, containerName: dbName, entityName: tableName });
		},

		error(error) {
			logger.log('error', createError(error), title);
		}
	};
};

const createError = (error) => {
	message = error.message || error.msg || error.errmsg;

	return {
		message,
		code: error.code,
		stack: error.stack,
	};
}


const logHelper = {
	getSystemInfo,
	createLogger,
};

module.exports = logHelper;
