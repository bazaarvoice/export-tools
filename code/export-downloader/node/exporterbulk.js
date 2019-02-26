#!/usr/bin/env node

const ArgumentParser = require('argparse').ArgumentParser;
const dce = require('./exporter-common.js');
const util = require('util');
var errors = require('request-promise/errors');

const parser = new ArgumentParser({
  version: '1.0.0',
  addHelp: true,
  description: 'Bazaarvoice Exporter bulk download tool'
});
parser.addArgument(['--config' ],
  {
    help: 'path to configuration (default is ../config.json)',
    defaultValue: '../config.json'
  });
parser.addArgument(['--dest'],
  {
    help: 'destination folder to store downloaded data'
  });
parser.addArgument(['--env'],
  {
    help: 'environment of Exporter service (must be present in config file)',
    required: true
  });
parser.addArgument(['--date'],
  {
    help: 'date of files to download, in YYYY-mm-dd format',
    required: true
  });
parser.addArgument(['--type' ],
  {
    help: 'type of files, like reviews, questions... (defaults to all types)',
    defaultValue: 'all'
  });
parser.addArgument(['--v' ],
  {
    help: 'version of data to retrieve',
    defaultValue: 'v1'
  });
parser.addArgument(['--debug' ],
  {
    help: 'enable debugs',
    action: 'storeTrue'
  });
parser.addArgument(['--fulls' ],
  {
    help: 'Retrieve fulls',
    action: 'storeTrue'
  });
parser.addArgument(['--incrementals' ],
  {
    help: 'Retrieve incrementals',
    action: 'storeTrue'
  });

const options = parser.parseArgs();

// Download all existing manifests and find the one corresponding to version, date and type.
// Returns: Promise of manifest path or throws Error
const getManifestPath = (environment, version, date, type) => {
  return exporter.doHttpGet(environment.url, environment.passkey, environment.secret, null)
    .then(function(response) {
      const manifests = JSON.parse(response.body);
      if (options.debug) {console.log(JSON.stringify(manifests, null, 4));}

      const manifestPath = exporter.getManifestForDate(manifests['manifests'], version, date, type);
      if (!manifestPath) {
        throw new Error(util.format('Warning: did not find "%s" for version="%s" type="%s" in downloaded manifests', date, version, type));
      }
      if (options.debug) {console.log('manifestPath=' + manifestPath);}

      return manifestPath;
    });

};

// Retrieve manifest contents
// Returns: Promise of manifest json object
const getManifestFile = (environment, manifestPath) => {
  return exporter.doHttpGet(environment.url, environment.passkey, environment.secret, manifestPath)
    .then(function(response) {
      const manifest = JSON.parse(response.body);
      if (options.debug) {console.log(JSON.stringify(manifest, null, 4));}

      return manifest;
    });

};

// Get array of files to download, from manifest list of files, filtered by category (or 'all')
// Returns: array of strings, each representing a DCE file path
const getFilesToDownload = (manifest, category) => {
  const files = [];
  Object.keys(manifest).forEach(function(fileType) {
    if (fileType === category || category === 'all') {
      manifest[fileType].forEach(function(entry) {
        files.push(entry.path);
      });
    }
  });
  if (options.debug) {console.log('Files to download:' + JSON.stringify(files, null, 4));}
  return files;
};

// Download and save a file.
// Returns: Promise of the file name
const downloadFile = (environment, destination, path) => {
  if (options.debug) {console.log('File to download:' + path);}
  return exporter.doHttpGet(environment.url, environment.passkey, environment.secret, path)
    .then(function(response) {
      exporter.saveFile(destination, path, response.body);

      return path;
    });
};

const downloadBulk = (environment, version, date, category, destination, dataType) => {
  getManifestPath(environment, version, date, dataType)
    .then(function(manifestPath) { return getManifestFile(environment, manifestPath);})
    .then(function(manifest) { return getFilesToDownload(manifest, category);})
    .each(function(file) { return downloadFile(environment, destination, file);})
    .then(function(done) { if (options.debug) {console.log('Done downloading: '+JSON.stringify(done, null, 4));}})
    .catch(errors.StatusCodeError, function (reason) {
      console.error(util.format('Failed HTTP call: %d %s', reason.statusCode, reason.response.statusMessage));
      if (options.debug) {console.error(reason);}
      process.exit(1);
    })
    .catch(errors.RequestError, function (reason) {
      // The request failed due to technical reasons.
      // reason.cause is the Error object Request would pass into a callback.
      console.error(util.format('Failure %s', reason.cause.message));
      if (options.debug) {console.error(reason);}
    })
    .catch(function (error) {
      console.error(error.message);
      if (options.debug) {console.error(error);}
    });

};

const config = exporter.readConfig(options.config);

if (!options.fulls && !options.incrementals) {
  console.error('Must specified one or both of [--fulls, --increments]');
  process.exit(1);
}

const environment = exporter.getEnvironment(config, options.env) || process.exit(1);

if (options.fulls) {
  downloadBulk(environment, options.v, options.date, options.type, options.dest, 'fulls');
}
if (options.incrementals) {
  downloadBulk(environment, options.v, options.date, options.type, options.dest, 'incrementals');
}
