#!/usr/bin/env node

const request = require('request');
const options = require('commander');
const crypto = require('crypto');
const util = require('util');
const pathLib = require('path');
const fs = require('fs');
const zlib = require('zlib');

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

function doHttpGet(uri, passkey, secretKey, path, callback) {
    const timestamp = new Date().getTime();

    const signature = createSignature(passkey,  secretKey,  timestamp,  path);

    if (path) {
        console.log("Downloading " + path);
    } else {
        console.log("Retrieving manifests");
    }

    request({
        uri: path ? uri + "?path=" + path : uri,
        followRedirect: true,
        maxRedirects: 2,
        headers: buildBVHeaders(passkey, signature, timestamp),
        qs : path ? { path: path } : {}
    }, callback);

}

function saveFile(dest, path, content) {
    const file = pathLib.join(dest, path);
    console.log("Saving as " + file);

    const dirname = pathLib.dirname(file);

    // Use reduce() to recursively create the entire directory path
    dirname.split(pathLib.sep).reduce(function(parentDir, childDir) {
        const curDir = pathLib.resolve(parentDir, childDir);
        if (!fs.existsSync(curDir)) {
            fs.mkdirSync(curDir);
        }

        return curDir;
    }, pathLib.isAbsolute(dirname) ? pathLib.sep : '');

    fs.writeFile(file, content, function(err) {
        if (err) {
            console.error("Error writing " + file + " : " + err);
        }
    });
}

options
    .version('1.0.0')
    .option('-p, --path [DCE_file]', 'DCE file path')
    .option('-c, --config [configFile]', 'path to configuration (default is ../config.json)', '../config.json')
    .option('-d, --dest [directory]', 'destination folder to store downloaded data')
    .option('-e, --env <environment>', 'environment of DCE service (must be present in config file)')
    .parse(process.argv);

const config = readConfig(options.config);

const destination = options.dest;
const path = options.path;

if (!(options.env in config)) {
    console.error("Environment %s not found in config", options.env);
    process.exit(1);
}

const environment = config[options.env];
const passkey = environment.passkey;
const secretKey = environment.secret;
const uri = environment.url;

doHttpGet(uri, passkey, secretKey, path, function(error, response, body) {
    if (error || response.statusCode != 200) {
        console.error("Error retrieving data: " + error);
        process.exit(1);

    } else if (destination && path) {
        // Write to file, if requested to do so.
        saveFile(destination, path, body);

    } else if (path && path.includes("gz")) {

        // gzip-style decompression
        zlib.gunzip(body, function (err, result) {
            if (err) {
                console.log(body);
            } else {
                console.log(JSON.stringify(result, null, 4));
            }
        });

    } else {
        const parsed = JSON.parse(body);
        console.log(JSON.stringify(parsed, null, 4));
    }

});

