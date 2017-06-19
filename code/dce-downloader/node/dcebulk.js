#!/usr/bin/env node

const request = require('request');
const options = require('commander');
const crypto = require('crypto');
const util = require('util');
const pathLib = require('path');
const fs = require('fs');
const zlib = require('zlib');
const Promise = require('bluebird');

function readConfig(config) {
    try {
        return JSON.parse(require('fs').readFileSync(config, 'utf8'));
    } catch (e) {
        console.error("Error parsing %s: %s", options.config, e.toString());
        process.exit(1);
    }
}

// Create message to sign
function createMessage(passkey, timestamp, path) {
    // If 'path' parameter will be in the GET request, we must include it as part of the HMAC signature.

    if (path) {
        return util.format("path=%s&passkey=%s&timestamp=%d", path, passkey, timestamp);
    } else {
        return util.format("passkey=%s&timestamp=%d", passkey, timestamp);
    }
}

// Create hmac signature
function createSignature(passkey, secretKey, timestamp, path) {
    //return crypto.HmacSHA256(createMessage(passkey, timestamp, path), secretKey).toString();
    return crypto.createHmac('sha256', secretKey)
        .update(createMessage(passkey, timestamp, path))
        .digest('hex');
}

function buildBVHeaders(passkey, signature, timestamp) {
    return {'X-Bazaarvoice-Passkey': passkey, 'X-Bazaarvoice-Signature': signature, 'X-Bazaarvoice-Timestamp': timestamp};
}

function doHttpGet(environment, path, callback) {
    const timestamp = new Date().getTime();

    const signature = createSignature(environment.passkey, environment.secret, timestamp,  path);

    if (path) {
        console.log("Downloading " + path);
    } else {
        console.log("Retrieving manifests");
    }

    request({
        uri: path ? environment.url + "?path=" + path : environment.url,
        followRedirect: true,
        maxRedirects: 2,
        headers: buildBVHeaders(environment.passkey, signature, timestamp),
        qs : path ? { path: path } : {}
    }, callback);

}

function mkdirs(dirname) {
    // Use reduce() to recursively create the entire directory path
    dirname.split(pathLib.sep).reduce(function(parentDir, childDir) {
        const curDir = pathLib.resolve(parentDir, childDir);
        if (!fs.existsSync(curDir)) {
            fs.mkdirSync(curDir);
        }

        return curDir;
    }, pathLib.isAbsolute(dirname) ? pathLib.sep : '');

}
function saveFile(dest, path, content) {
    const file = pathLib.join(dest, path);
    console.log("   Saving as " + file);

    mkdirs(pathLib.dirname(file));

    fs.writeFile(file, content, function(err) {
        if (err) {
            throw new Error("Error writing " + file + " : " + err);
        }
    });
}

function getManifestForDate(manifestArray, version, date, type) {
    // Find the manifest object that matches version of type
    const manifests = manifestArray.find(function(manifests) {
        return manifests.version == version && type in manifests;
    });

    // Find the specific manifest that matches the date.
    const item = manifests ? manifests[type].find(function(item) {return item.date == date;}) : null;

    // Return the path for this manifest.
    return item ? item.path : null;
}

function getManifestPath(environment, version, date, type) {
    return new Promise(function(resolve, reject) {
        doHttpGet(environment, null, function(error, response, body) {
            if (error || response.statusCode != 200) {
                throw new Error("Error retrieving data: " + error);
            }
            const manifests = JSON.parse(body);
            if (options.debug) console.log(JSON.stringify(manifests, null, 4));

            const manifestPath = getManifestForDate(manifests['manifests'], version, date, type);
            if (!manifestPath) {
                throw new Error("Error: did not find " + date + " in downloaded manifests");
            }

            resolve(manifestPath);
        });

    });
}

function getManifestFile(environment, manifestPath) {
    return new Promise(function(resolve, reject) {
        doHttpGet(environment, manifestPath, function(error, response, body) {
            if (error || response.statusCode != 200) {
                throw new Error("Error retrieving data: " + error);
            }
            const manifest = JSON.parse(body);
            if (options.debug) console.log(JSON.stringify(manifest, null, 4));

            resolve(manifest);
        });

    });
}

function getFilesToDownload(manifest, category) {
    return new Promise(function(resolve, reject) {
        const files = [];
        Object.keys(manifest).forEach(function(fileType) {
            if (fileType == category || category == "all") {
                manifest[fileType].forEach(function(entry) {
                    files.push(entry.path);
                });
            }
        });
        if (options.debug) console.log("Files to download:" + JSON.stringify(files, null, 4));
        resolve(files);
    });
}

function downloadFile(environment, destination, path) {
    return new Promise(function(resolve, reject) {
        doHttpGet(environment, path, function(error, response, body) {
            if (error || response.statusCode != 200) {
                throw new Error("Error retrieving data: " + error);
            }
            saveFile(destination, path, body);

            resolve(path);
        });

    });
}
options
    .version('1.0.0')
    .option('-c, --config [configFile]', 'path to configuration (default is ../config.json)', '../config.json')
    .option('-d, --dest [directory]', 'destination folder to store downloaded data (defaults to ./output)', './output')
    .option('-e, --env <environment>', 'environment of DCE service (must be present in config file)')
    .option('--date <date>', 'date of files to download, in YYYY-mm-dd format')
    .option('--type [type]', 'type of files, like reviews, questions... (defaults to all types)', 'all')
    .option('--version [version]', 'version of data to retrieve (defaults to v2)', 'v2')
    .option('--debug', 'enable debugs')
    .parse(process.argv);

const config = readConfig(options.config);

const destination = options.dest;
const date = options.date;
const category = options.type;
const version = options.version;

if (!(options.env in config)) {
    console.error("Environment %s not found in config", options.env);
    process.exit(1);
}

const environment = config[options.env];

// TODO: support 'partials' in addition to 'fulls'
getManifestPath(environment, version, date, 'fulls')
    .then(function(manifestPath) { return getManifestFile(environment, manifestPath)})
    .then(function(manifest) { return getFilesToDownload(manifest, category);})
    .mapSeries(function(file) { return downloadFile(environment, destination, file); })
    .then(function(done) { if (options.debug) console.log("Done downloading:"+JSON.stringify(done, null, 4));})
    .catch(function (error) {
        console.error(error);
        process.exit(1);
    });

