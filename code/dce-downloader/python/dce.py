#!/usr/bin/env python
"""
title: DCE Downloader
description: Downloads DCE data from DCE service in BV
"""
import argparse
import os
import time
import hmac
import hashlib
import requests
import logging
import json
import zlib

#logging.basicConfig(level=logging.DEBUG)

# Create hmac signature
def createSignature(passkey, secretKey, timestamp, path):
    # If 'path' parameter will be in the GET request, we must include it as part of the HMAC signature.

    if path:
        message = "path={0}&passkey={1}&timestamp={2}".format(path, passkey, timestamp)
    else:
        message = "passkey={0}&timestamp={1}".format(passkey, timestamp)

    return hmac.new(secretKey, message, hashlib.sha256).hexdigest()

def buildBVHeaders(passkey, signature, timestamp):
    return {'X-Bazaarvoice-Passkey': passkey, 'X-Bazaarvoice-Signature': signature, 'X-Bazaarvoice-Timestamp': timestamp}

def doHttpGet(url, passkey, secretKey, path):
    timestamp = str(round(time.time() * 1000))

    # Get current manifests
    signature = createSignature(passkey=passkey, secretKey=secretKey, timestamp=timestamp, path=path)

    headers = buildBVHeaders(passkey, signature, timestamp)

    params = { 'path' : path } if path else {}

    if path:
        print "Downloading " + path;
    else:
        print "Retrieving manifests"

    resp = requests.get(url, params=params, headers=headers, timeout=60, allow_redirects=True, stream=True)
    return resp

def saveFile(dest, path, content):
    file = dest + path
    print "Saving as " + file

    dirname = os.path.dirname(file)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(file, "wb") as file:
        file.write(content)


# Main part
if __name__ == '__main__':
    # Setting script parameters and variables
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('-config',  dest='configFile', help='path to configuration (default is ../config.json)')
    p.add_argument('-env',  dest='environment', help='environment of DCE service (must be present in config file)')
    p.add_argument('-path', dest='path', help='DCE file')
    p.add_argument('-dest', dest='destination', help='destination folder to store downloaded data')
    opts = p.parse_args()

    # Determine operation mode or print help
    if not opts.environment:
        print "Required -env not specified"
        p.print_help()
        exit(1)

    configFile = opts.configFile if opts.configFile else "../config.json"

    if not os.path.isfile(configFile):
        exit("Config file \"" + configFile + "\" does not exist")
    else:
        with open(configFile) as key_file:
            config = json.load(key_file)

    destination = opts.destination.rstrip('\\') if opts.destination else None
    path = opts.path if opts.path else None
    environment = config[opts.environment] if opts.environment in config else None

    if not environment:
        print "Error: environment " + opts.environment + " not present in " + configFile
        exit(1)

    passkey = str(environment['passkey']).strip('"')
    secretKey = str(environment['secret']).strip('"')
    url = str(environment['url']).strip('"')

    resp = doHttpGet(url, passkey, secretKey, path)
    if resp.status_code != requests.codes.ok:
        exit(resp.content)

    # Write to file, if requested to do so.
    if destination and path:
        saveFile(destination, path, resp.content)

    elif path and "gz" in path:

        # gzip-style decompression

        data = zlib.decompress(resp.content, zlib.MAX_WBITS|32)
        try:
            # Try to pretty print it, in case it's valid json

            parsed = json.loads(data);
            print json.dumps(parsed, indent=4, sort_keys=True)

        except Exception as e:
            # Not valid json contents. Just print it as it was recevied

            print data

    else:
        parsed = json.loads(resp.content);
        print json.dumps(parsed, indent=4, sort_keys=True)
