#!/usr/bin/env python26
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

#logging.basicConfig(level=logging.DEBUG)

# host url for DCE services
DCE_HOSTS = {
    "stg": "data-stg.nexus.bazaarvoice.com",
    "prod": "data.nexus.bazaarvoice.com"
}

# Main part
if __name__ == '__main__':
    # Setting script parameters and variables
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('-key-path',  dest='keypath', help='path to json file which includes x-api-key and shared key. '
                                                      'Refer to keys.json, which is used if not specified')
    p.add_argument('-env',  dest='environment', help='environment of DCE service, stg or prod.')
    p.add_argument('-path', dest='path', help='path to target file')
    p.add_argument('-dest', dest='destination', help='destination folder to store downloaded data')
    p.add_argument('--run', dest='run', action='store_true', help="Need to specify for actual execution!")
    opts = p.parse_args()

    # Determine operation mode or print help
    if not opts.run:
        p.print_help()
        exit(1)

    if not opts.environment or opts.environment not in ['stg', 'prod']:
        p.print_help()
        exit(1)

    if not opts.keypath:
        keypath = "../keys.json"
    else:
        keypath = opts.keypath

    if not os.path.isfile(keypath):
        exit("Key file \"" + keypath + "\" does not exist")
    else:
        with open(keypath) as key_file:
            keys = json.load(key_file)

    if opts.destination:
        if not os.path.isdir(opts.destination):
            exit("Destination directory \""+opts.destination + "\" does not exist")
        dest = opts.destination.rstrip('\\')
    else:
        dest = "."

    xApiKey = json.dumps(keys[opts.environment]["x-api-key"]).strip('"')
    sharedKey = json.dumps(keys[opts.environment]["secret"]).strip('"')
    timestamp = str(round(time.time() * 1000))
    message = "x-api-key={0}&timestamp={1}".format(xApiKey, timestamp)
    url = "https://{0}/v1/dce/data".format(DCE_HOSTS[opts.environment])

    if opts.path:
        message ="path={0}&{1}".format(opts.path, message)
        url = "{0}?path={1}".format(url, opts.path);
        file = opts.path.split('/')[-1]
        print "Downloading " + opts.path + " to " + dest + "/"+file
        # cd to destination folder
        os.chdir(dest)
    else:
        print "Retrieving dates"

    signed = hmac.new(sharedKey, message, hashlib.sha256).hexdigest()
    headers = {'x-api-key': xApiKey, 'BV-DCE-ACCESS-SIGN': signed, 'BV-DCE-ACCESS-TIMESTAMP': timestamp}
    resp = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    if resp.status_code != requests.codes.ok:
        exit(resp.content)

    if opts.path:
        with open(file, "wb") as file:
            file.write(resp.content)
    else:
        print resp.content
