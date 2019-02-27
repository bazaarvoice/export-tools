#!/usr/bin/env node

const ArgumentParser = require('argparse').ArgumentParser;
const zlib = require('zlib');
const exporter = require('./exporter-common.js');
const errors = require('request-promise/errors');
const util = require('util');

const parser = new ArgumentParser({
  version: '1.0.0',
  addHelp: true,
  description: 'Bazaarvoice Exporter download tool'
});
parser.addArgument(['--path'],
  {
    help: 'Exporter file path'
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

const options = parser.parseArgs();
const config = exporter.readConfig(options.config);

const destination = options.dest;
const path = options.path;

const environment = exporter.getEnvironment(config, options.env) || process.exit(1);

exporter.doHttpGet(environment.url, environment.passkey, environment.secret, path)
  .then(function(response) {
    if (destination) {
      // Write to file, if requested to do so.
      exporter.saveFile(destination, path || 'manifests', response.body);

    } else if (path && path.includes('gz')) {

      // gzip-style decompression
      zlib.gunzip(response.body, function (err, result) {
        if (err) {
            console.log('could not gunzip');
        } else {
            // Body was an octet stream. We need to convert result to String
            // then do a JSON stringify.
            console.log(JSON.stringify(result.toString(), null, 4));
        }
      });

    } else {
      try {
        const parsed = JSON.parse(response.body);
        console.log(JSON.stringify(parsed, null, 4));
      } catch (e) {
        console.log(response.body);
      }
    }

  })
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
    process.exit(1);
  })
  .catch(function (error) {
    console.error(error.message);
    if (options.debug) {console.error(error);}
    process.exit(1);
  });


