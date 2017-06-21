#!/usr/bin/env node

const options = require('commander');
const zlib = require('zlib');
const dce = require('./dce-common.js');
const errors = require('request-promise/errors');
const util = require('util');

options
    .version('1.0.0')
    .option('-p, --path [DCE_file]', 'DCE file path')
    .option('-c, --config [configFile]', 'path to configuration (default is ../config.json)', '../config.json')
    .option('-d, --dest [directory]', 'destination folder to store downloaded data')
    .option('-e, --env <environment>', 'environment of DCE service (must be present in config file)')
    .parse(process.argv);

const config = dce.readConfig(options.config);

const destination = options.dest;
const path = options.path;

const environment = dce.getEnvironment(config, options.env) || process.exit(1);

dce.doHttpGet(environment.url, environment.passkey, environment.secret, path)
    .then(function(response) {
        if (destination) {
            // Write to file, if requested to do so.
            dce.saveFile(destination, path || "manifests", response.body);

        } else if (path && path.includes("gz")) {

            // gzip-style decompression
            zlib.gunzip(response.body, function (err, result) {
                if (err) {
                    console.log(response.body);
                } else {
                    console.log(JSON.stringify(result, null, 4));
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


