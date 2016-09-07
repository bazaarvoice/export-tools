#!/usr/bin/env python26
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
import logging
import json

# logging.basicConfig(level=logging.DEBUG)

# host url for DCE services
DCE_HOSTS = {
    "stg": "data-stg.nexus.bazaarvoice.com",
    "prod": "data.nexus.bazaarvoice.com"
}

DCE_KEYS = {
    "stg": {"x-api-key": "x-api-key", "secret": "shared secret"},
    "prod": {"x-api-key": "x-api-key", "secret": "shared secret"}
}

# Main part
if __name__ == '__main__':
    # Setting script parameters and variables
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('-env',  dest='environment', help='environment of DCE service, stg or prod.')
    p.add_argument('-date', dest='date', help='date')
    p.add_argument('-version', dest='version', help='version of target files, like v1, v2 ...')
    p.add_argument('-dest', dest='destination', help='destination folder to store downloaded data, current folder is used if not present')
    p.add_argument('-cat', dest='category', help='category of target files, like reviews, questions...')
    p.add_argument('--run', dest='run', action='store_true', help="Need to specify for actual execution!")
    opts = p.parse_args()

    # Determine operation mode or print help
    if not opts.run:
        p.print_help()
        exit(1)

    if not opts.environment or opts.environment not in ['stg', 'prod']:
        p.print_help()
        exit(1)

    if not opts.date or not opts.version:
        p.print_help()
        exit(1)

    if opts.destination:
        dest = opts.destination
    else:
        dest = "."

    timestamp = int(round(time.time() * 1000))
    path="path=/manifests/{0}/{1}/manifest.json".format(opts.date, opts.version)
    message = "{0}&x-api-key={1}&timestamp={2}".format(path, DCE_KEYS[opts.environment]["x-api-key"], timestamp)
    url = "https://{0}/v1/dce/data?{1}".format(DCE_HOSTS[opts.environment], path)
    # cd to destination folder
    os.chdir(dest)

    signed = hmac.new(DCE_KEYS[opts.environment]["secret"], message, hashlib.sha256).hexdigest()
    headers = {'x-api-key': DCE_KEYS[opts.environment]["x-api-key"], 'BV-DCE-ACCESS-SIGN': signed, 'BV-DCE-ACCESS-TIMESTAMP': timestamp}
    resp = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    decoded_json = json.loads(resp.content)
    print decoded_json

    for majorkey, subdict in decoded_json.iteritems():
        print "####################### " + majorkey + " ######################"

        if opts.category and opts.category != majorkey:
            continue
        # create directory if not exist
        if not os.path.exists(majorkey):
            os.makedirs(majorkey)
        # cd in folder
        os.chdir(majorkey)

        for subkey, value in subdict.iteritems():
            print "Start to download " + subkey + " ..."
            timestamp = int(round(time.time() * 1000))
            path="path={0}".format(subkey)
            message = "{0}&x-api-key={1}&timestamp={2}".format(path, DCE_KEYS[opts.environment]["x-api-key"], timestamp)
            url = "https://{0}/v1/dce/data?{1}".format(DCE_HOSTS[opts.environment], path)
            signed = hmac.new(DCE_KEYS[opts.environment]["secret"], message, hashlib.sha256).hexdigest()
            headers = {'x-api-key': DCE_KEYS[opts.environment]["x-api-key"], 'BV-DCE-ACCESS-SIGN': signed, 'BV-DCE-ACCESS-TIMESTAMP': timestamp}
            r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
            with open(subkey.split('/')[-1], "wb") as file:
                file.write(r.content)
            print "Done."
        # cd back to parent
        os.chdir("..")