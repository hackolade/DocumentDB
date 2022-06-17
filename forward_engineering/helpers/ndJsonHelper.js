const fs = require('fs');
const { createInterface } = require('readline');

const createNdJsonStream = (ndJsonFilePath, onData) => {
    const stream = fs.createReadStream(ndJsonFilePath);

    return createInterface({ input: stream, crlfDelay: Infinity })
        .on('line', data => {
            onData(data);
        });
};

const shouldLogStep = line => {
    if (line < 1000) {
        return line % 100 === 0;
    } else {
        return line % 1000 === 0;
    }
};

const getCountOfLines = filePath =>
    new Promise((resolve, reject) => {
        let countOfLines = 0;

        try {
            createNdJsonStream(filePath, () => { countOfLines++; })
                .on('close', () => {
                    resolve(countOfLines);
                });
        } catch (error) {
            reject(createFileError(error));
        }
    });

const createFileError = error => {
    const fileError = new Error(error);

    fileError.type = 'file';

    return fileError;
};

const readNdJsonByLine = async (filePath, log) => {
    const maxDocuments = await getCountOfLines(filePath);
    let line = 0;
    const documents = [];
    return new Promise((resolve, reject) => {
        try {
            createNdJsonStream(filePath, async data => {
                line++;

                if (shouldLogStep(line)) {
                    log.info( `NDJSON_READ_LINES - lines: ${line} / ${maxDocuments}, progress: ${line / maxDocuments}`);
                }
                if (data) {
                    documents.push(data);
                }
            }).on('close', () => {
                log.info(`NDJSON_READ_LINES - lines: ${line} / ${maxDocuments}, progress: ${line / maxDocuments}`);
                return resolve(documents);
            })
        } catch (error) {
            return reject(error);
        }
    });
};

module.exports = readNdJsonByLine;
