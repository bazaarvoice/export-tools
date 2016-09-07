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

#logging.basicConfig(level=logging.DEBUG)

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

    if opts.destination:
        dest = opts.destination
    else:
        dest = "."

    timestamp = int(round(time.time() * 1000))
    message = "x-api-key={0}&timestamp={1}".format(DCE_KEYS[opts.environment]["x-api-key"], timestamp)
    url = "https://{0}/v1/dce/data".format(DCE_HOSTS[opts.environment])

    if opts.path:
        message ="path={0}&{1}".format(opts.path, message)
        url = "{0}?path={1}".format(url, opts.path);
        # cd to destination folder
        os.chdir(dest)

    signed = hmac.new(DCE_KEYS[opts.environment]["secret"], message, hashlib.sha256).hexdigest()
    headers = {'x-api-key': DCE_KEYS[opts.environment]["x-api-key"], 'BV-DCE-ACCESS-SIGN': signed, 'BV-DCE-ACCESS-TIMESTAMP': timestamp}
    r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    if opts.path:
        with open(opts.path.split('/')[-1], "wb") as file:
            file.write(r.content)
    else:
        print r.content
