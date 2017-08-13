#!/usr/bin/env python
"""
title: DCE Downloader
description: Downloads DCE data from DCE service in BV in bulk
"""
import argparse
import os
import time
import hmac
import hashlib
import requests
import json

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

    resp = requests.get(url, params=params, headers=headers, timeout=60, allow_redirects=True, stream=True)
    return resp

def getManifestForDate(manifests, version, date, dataType):
    for manifest_record in manifests:
        if manifest_record['version'] == version and dataType in manifest_record:
            for item in manifest_record[dataType]:
                if item['date'] == date:
                    return item['path']

    return None

def saveFile(dest, path, content):
    filename_ = dest + path
    print "Saving as " + filename_

    dirname_ = os.path.dirname(filename_)
    if not os.path.exists(dirname_):
        os.makedirs(dirname_)

    with open(filename_, "wb") as file_:
        file_.write(content)

def getFiles(manifests, version, date, category, destination, dataType):
    manifest_path = getManifestForDate(manifests, version, date, dataType)
    if not manifest_path:
        print "Warning: Did not find \"" + date + "\" for version=\"" + version + "\" type=\"" + dataType + "\" in downloaded manifests"
        return False

    # We have the manifest file path for the requested date and version. Download it and process.

    print "Fetching " + manifest_path + "..."
    resp = doHttpGet(url, passkey, secretKey, manifest_path)
    if resp.status_code != requests.codes.ok:
        print "Error: could not download " + manifest_path + " (" + str(resp.status_code) + ")"
        exit(resp.content)

    file_type_map = json.loads(resp.content)

    if category == "all":
        print "Downloading all categories..."
    else:
        print "Downloading category \"" + category + "\"..."

    # Iterate through all category types, processing the category we want
    for file_type in file_type_map.keys():
        if file_type == category or category == "all":
            for file_object in file_type_map[file_type]:
                path = file_object['path']
                print "Fetching " + path + " ..."

                resp = doHttpGet(url, passkey, secretKey, path)
                if resp.status_code != requests.codes.ok:
                    print "Error: could not download " + path + " (" + str(resp.status_code) + ")"
                    exit(resp.content)

                saveFile(destination, path, resp.content)

    return True

# Main part
if __name__ == '__main__':
    # Setting script parameters and variables
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--config',  dest='configFile', help='path to configuration (default is ../config.json)')
    p.add_argument('--env',  dest='environment', required=True, help='environment of DCE service (must be present in config file)')
    p.add_argument('--date', dest='date', required=True, help='date of files to download, in YYYY-mm-dd format')
    p.add_argument('--dest', dest='destination', required=True, help='destination folder to store downloaded data')
    p.add_argument('--type', dest='category', help='type of files, like reviews, questions... (defaults to all types)')
    p.add_argument('--v', dest='version', help='version of data to retrieve (defaults to v2)')
    p.add_argument('--fulls', dest='fulls', action='store_true', help='Retrieve fulls')
    p.add_argument('--incrementals', dest='incrementals', action='store_true', help='Retrieve incrementals')
    opts = p.parse_args()

    # Determine operation mode or print help
    if not opts.environment or not opts.date:
        p.print_help()
        exit(1)

    configFile = opts.configFile if opts.configFile else "../config.json"
    version = opts.version if opts.version else "v2"
    date = opts.date
    category = opts.category if opts.category else "all"
    destination = opts.destination.rstrip('\\') if opts.destination else "./output"
    fulls = opts.fulls
    incrementals = opts.incrementals

    if not fulls and not incrementals:
        exit("Must specify one or both of [--fulls, --incrementals]")

    if not os.path.isfile(configFile):
        exit("Config file \"" + configFile + "\" does not exist")
    else:
        with open(configFile) as key_file:
            config = json.load(key_file)

    environment = config[opts.environment] if opts.environment in config else None

    if not environment:
        print "Error: environment " + opts.environment + " not present in " + configFile
        exit(1)

    passkey = str(environment['passkey']).strip('"')
    secretKey = str(environment['secret']).strip('"')
    url = str(environment['url']).strip('"')

    print "Fetching manifests..."
    resp = doHttpGet(url, passkey, secretKey, None)
    if resp.status_code != requests.codes.ok:
        print "Error: could not download manifests (" + str(resp.status_code) + ")"
        exit(resp.content)

    manifest_json = json.loads(resp.content)
    manifests = manifest_json['manifests']

    if fulls:
        getFiles(manifests, version, date, category, destination, 'fulls')

    if incrementals:
        getFiles(manifests, version, date, category, destination, 'incrementals')

    exit(0)
