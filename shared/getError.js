const getError = error => (error instanceof Error ? error : new Error(error));

module.exports = { getError };
