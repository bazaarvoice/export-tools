#!/usr/bin/env node

const options = require('commander');
const dce = require('./dce-common.js');
const util = require('util');
var errors = require('request-promise/errors');

// Download all existing manifests and find the one corresponding to version, date and type.
// Returns: Promise of manifest path or throws Error
function getManifestPath(environment, version, date, type) {
    return dce.doHttpGet(environment.url, environment.passkey, environment.secret, null)
        .then(function(response) {
            const manifests = JSON.parse(response.body);
            if (options.debug) console.log(JSON.stringify(manifests, null, 4));

            const manifestPath = dce.getManifestForDate(manifests['manifests'], version, date, type);
            if (!manifestPath) {
                throw new Error(util.format("Error: did not find \"%s\" for version=\"%s\" type=\"%s\" in downloaded manifests", date, version, type));
            }
            if (options.debug) console.log("manifestPath=" + manifestPath);

            return manifestPath;
        });

}

// Retrieve manifest contents
// Returns: Promise of manifest json object
function getManifestFile(environment, manifestPath) {
    return dce.doHttpGet(environment.url, environment.passkey, environment.secret, manifestPath)
        .then(function(response) {
            const manifest = JSON.parse(response.body);
            if (options.debug) console.log(JSON.stringify(manifest, null, 4));

            return manifest;
        });

}

// Get array of files to download, from manifest list of files, filtered by category (or 'all')
// Returns: array of strings, each representing a DCE file path
function getFilesToDownload(manifest, category) {
    const files = [];
    Object.keys(manifest).forEach(function(fileType) {
        if (fileType == category || category == "all") {
            manifest[fileType].forEach(function(entry) {
                files.push(entry.path);
            });
        }
    });
    if (options.debug) console.log("Files to download:" + JSON.stringify(files, null, 4));
    return files;
}

// Download and save a file.
// Returns: Promise of the file name
function downloadFile(environment, destination, path) {
    if (options.debug) console.log("File to download:" + path);
    return dce.doHttpGet(environment.url, environment.passkey, environment.secret, path)
        .then(function(response) {
            dce.saveFile(destination, path, response.body);

            return path;
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

const config = dce.readConfig(options.config);

const destination = options.dest;
const date = options.date;
const category = options.type;
const version = options.version;

const environment = dce.getEnvironment(config, options.env) || process.exit(1);

// For convenience, we're using Promises.
// TODO: support 'incrementals' in addition to 'fulls'
getManifestPath(environment, version, date, 'fulls')
    .then(function(manifestPath) { return getManifestFile(environment, manifestPath)})
    .then(function(manifest) { return getFilesToDownload(manifest, category);})
    .each(function(file) { return downloadFile(environment, destination, file);})
    .then(function(done) { if (options.debug) console.log("Done downloading: "+JSON.stringify(done, null, 4));})
    .catch(errors.StatusCodeError, function (reason) {
        console.error(util.format("Failed HTTP call: %d %s", reason.statusCode, reason.response.statusMessage));
        if (options.debug) console.error(reason);
        process.exit(1);
    })
    .catch(errors.RequestError, function (reason) {
        // The request failed due to technical reasons.
        // reason.cause is the Error object Request would pass into a callback.
        console.error(util.format("Failure %s", reason.cause.message));
        if (options.debug) console.error(reason);
        process.exit(1);
    })
    .catch(function (error) {
        console.error(error.message);
        if (options.debug) console.error(error);
        process.exit(1);
    });
