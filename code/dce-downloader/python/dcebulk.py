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

# Main part
if __name__ == '__main__':
    # Setting script parameters and variables
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('-key-path',  dest='keypath', help='path to json file which includes x-api-key and shared key. '
                                                       'Refer to keys.json, which is used if not specified')
    p.add_argument('-env',  dest='environment', help='environment of DCE service, stg or prod.')
    p.add_argument('-manifest', dest='manifest', help='path of manifest.json')
    p.add_argument('-dest', dest='destination', help='destination folder to store downloaded data; current folder is used if not present')
    p.add_argument('-type', dest='category', help='type of files, like reviews, questions...')
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
    path= path="path={0}".format(opts.manifest)
    message = "{0}&x-api-key={1}&timestamp={2}".format(path, xApiKey, timestamp)
    url = "https://{0}/v1/dce/data?{1}".format(DCE_HOSTS[opts.environment], path)
    # cd to destination folder
    os.chdir(dest)

    signed = hmac.new(sharedKey, message, hashlib.sha256).hexdigest()
    headers = {'x-api-key': xApiKey, 'BV-DCE-ACCESS-SIGN': signed, 'BV-DCE-ACCESS-TIMESTAMP': timestamp}
    print "Start to download..."
    resp = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    if resp.status_code != requests.codes.ok:
        exit(resp.content)

    decoded_json = json.loads(resp.content)

    for majorkey, subdict in decoded_json.iteritems():
        if opts.category and opts.category != majorkey:
            continue
        print "####################### Downloading " + majorkey + " ######################"
        # create directory if not exist
        if not os.path.exists(majorkey):
            os.makedirs(majorkey)
        # cd in folder
        os.chdir(majorkey)

        for subkey, value in subdict.iteritems():
            timestamp = str(round(time.time() * 1000))
            path="path={0}".format(subkey)
            message = "{0}&x-api-key={1}&timestamp={2}".format(path, xApiKey, timestamp)
            url = "https://{0}/v1/dce/data?{1}".format(DCE_HOSTS[opts.environment], path)
            signed = hmac.new(sharedKey, message, hashlib.sha256).hexdigest()
            headers = {'x-api-key': xApiKey, 'BV-DCE-ACCESS-SIGN': signed, 'BV-DCE-ACCESS-TIMESTAMP': timestamp}

            print "Downloading " + subkey + " to " + dest + "/" + majorkey + "/" +subkey.split('/')[-1]
            r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
            if r.status_code != requests.codes.ok:
                print r.content
            else:
                with open(subkey.split('/')[-1], "wb") as file:
                    file.write(r.content)

                print "Done."
        # cd back to parent
        os.chdir("..")