# DCE Tools in Perl

Two Perl scripts files are provided to get data from DCE service in BV. Please refer to https://developer.bazaarvoice.com/docs/displayable_content_export/ for details.

## Prerequisite
1. You should have perl v5.18.2 or later installed.
2. You should have SHA module for Perl installed.
3. You should have JSON module installed. (For mac os, running "brew install cpanm"  and "sudo cpanm install JSON")
4. You should have a file called "keys.json" under "./dce-downloader/keys.json", which includes code snippet below.
   - Please replace "x-api-key" and "shared secret" with real x-api-key key and shared secret key in keys.json before running scripts .
    ```
    {
    "stg":
        {
            "x-api-key": "x-api-key",
            "secret": "shared secret"
        },
    "prod":
        {
            "x-api-key": "x-api-key",
            "secret": "shared secret"
        }
    }
    ```
    
## dce.pl

This script is used to get dates/versions available and get individual file.

1. Get dates/versions available   
   ```
   /dce.pl --env=<env> --key-path=<path to keys.json>
   ```
   - --key-path=<path to keys.json> : ../keys.json is used if not specified (optional) 
   - --env=<env> : stg or prod   
   
   Example:
     ```
     ./dce.pl --env=stg
     ```
     Expected:
     {"data":["/manifests/2016-07-25/v1/manifest.json","/manifests/2016-08-08/v1/manifest.json"]}
     
2. Get individual file
   ```
   ./dce.pl --env=<env> --path=<path> --dest=<dest> --key-path=<path to keys.json>
   ```
   - --key-path=<path to keys.json> : ../keys.json is used if not specified (optional) 
   - --env=<env> : stg or prod 
   - --path=<path> : path to file, which can be manifest.json or items found in manifest.json  
   - --dest=<dest> : target folder to store data. Current folder '.' is used if not specified. (optional)
   
   Example:
     ```
     ./dce.pl --env=stg --path=/manifests/2016-08-22/v1/manifest.json --dest=.
     ```
     Expected:
     manifest.json is downloaded at current folder.
     
    
## dcebulk.pl

- This script is used to get files in manifest.json in bulk.
  ```
  ./dcebulk.pl --env=<env> --manifest=<path of manifest.json> --dest=<dest> --type=<type> --key-path=<path to keys.json>
  ```
  - --key-path=<path to keys.json> : ../keys.json is used if not specified (optional) 
  - --env=<env> : stg or prod (must)
  - --manifest=<path of manifest.json> : path of manifest.json
  - --dest=<dest> : target folder to store data. Current folder '.' is used if not specified. (optional)
  - --type=<type> : data type to download. If not specified, all data are downloaded. (optional)
 

Example

1. Get all data on specific date
```
./dcebulk.pl --env=stg --manifest=/manifests/2016-08-22/v1/manifest.json --dest=.
```
  - This will download all data of v1 on 2016-08-22 . Downloaded files can be found at <dest>/<type>, like ./answers/, ./questions/, ./reviews/

2. Get 'reviews' on specific date
```
./dcebulk.pl --env=stg --manifest=/manifests/2016-08-22/v1/manifest.json --dest=. --type="reviews"
```
  - This will download all data of v1 on 2016-08-22 . Downloaded files for reviews can be found at <dest>/reviews/