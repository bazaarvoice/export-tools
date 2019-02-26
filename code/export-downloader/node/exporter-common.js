
const request = require('request-promise');
const crypto = require('crypto');
const util = require('util');
const pathLib = require('path');
const fs = require('fs');

module.exports = {
  readConfig: function readConfig(config) {
    try {
      return JSON.parse(fs.readFileSync(config, 'utf8'));
    } catch (e) {
      console.error('Error reading %s: %s', config, e.toString());
      process.exit(1);
    }
  },

  getEnvironment: function getEnvironment(config, env) {
    if (!(env in config)) {
      console.error(util.format('Environment "%s" not found in config', env));
      return undefined;
    }

    const environment = config[env];
    if (!environment.url ||
            !environment.passkey ||
            !environment.secret) {

      console.error(util.format('Environment "%s" is missing one or more properties: %s', env, JSON.stringify(environment)));
      return undefined;
    }
    return environment;
  },

  // Create message to sign
  createMessage: function createMessage(passkey, timestamp, path) {
    // If 'path' parameter will be in the GET request, we must include it as part of the HMAC signature.

    if (path) {
      return util.format('path=%s&passkey=%s&timestamp=%d', path, passkey, timestamp);
    } else {
      return util.format('passkey=%s&timestamp=%d', passkey, timestamp);
    }
  },

  // Create hmac 256 signature
  createSignature: function createSignature(message, secretKey) {
    return crypto.createHmac('sha256', secretKey)
      .update(message)
      .digest('hex');
  },

  // Create required Exporter headers
  // Returns: dictionary of required Exporter headers
  buildBVHeaders: function buildBVHeaders(passkey, signature, timestamp) {
    return {
      'X-Bazaarvoice-Passkey': passkey,
      'X-Bazaarvoice-Signature': signature,
      'X-Bazaarvoice-Timestamp': timestamp
    };
  },

  // Start a HTTP GET request to Exporter
  // Returns: Promise of a IncomingMessage object
  doHttpGet: function doHttpGet(uri, passkey, secret, path) {
    const timestamp = new Date().getTime();
    const message = this.createMessage(passkey, timestamp, path);
    const signature = this.createSignature(message, secret);

    if (path) {
      console.log('Downloading ' + path);
    } else {
      console.log('Retrieving manifests');
    }

    // Start a request as a Promise
    return request({
      uri: uri,
      followRedirect: true,
      maxRedirects: 2,
      headers: this.buildBVHeaders(passkey, signature, timestamp),
      qs: path ? { path: path } : {},
      resolveWithFullResponse: true,

      // If we're getting a gz file from Exporter, we need to preserve it as binary
      encoding : path && path.includes('gz') ? null : 'utf-8'
    });

  },

  // Recursively create a directly path, eg., all the parts of a/b/c/d/e/f/g/h/i/j ...
  mkdirs: function mkdirs(dirname) {
    // Use reduce() to recursively create the entire directory path
    dirname.split(pathLib.sep).reduce(function(parentDir, childDir) {
      const curDir = pathLib.resolve(parentDir, childDir);
      if (!fs.existsSync(curDir)) {
        fs.mkdirSync(curDir);
      }

      return curDir;
    }, pathLib.isAbsolute(dirname) ? pathLib.sep : '');

  },

  saveFile: function saveFile(dest, path, content) {
    const file = pathLib.join(dest, path);
    console.log('   Saving ' + file);

    // Make sure full directory path is created, then write to the file
    this.mkdirs(pathLib.dirname(file));

    fs.writeFile(file, content, function(err) {
      if (err) {
        throw new Error('Error writing ' + file + ' : ' + err);
      }
    });
  },

  // Get the manifest path, given an array of manifests, for a specific version, date and type
  // Returns: manifest path or undefined
  getManifestForDate: function getManifestForDate(manifestArray, version, date, type) {
    // Find the manifest object that matches version of type
    const manifests = manifestArray.find(function(manifests) {
      return manifests.version === version && type in manifests;
    });

    // Find the specific manifest that matches the date.
    const item = manifests ? manifests[type].find(function(item) {return item.date === date;}) : undefined;

    // Return the path for this manifest.
    return item ? item.path : undefined;
  }


};
