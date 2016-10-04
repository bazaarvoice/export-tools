# DCE Tools in Python

Two Python scripts files are provided to get data from DCE service in BV. Please refer to https://developer.bazaarvoice.com/docs/displayable_content_export/ for details.

## Prerequisite
1. You should have Python v2.7.10 or later installed.
2. You should have requests installed (run "pip install requests")
3. You should have a file called "keys.json" under "./dce-downloader/keys.json", which includes code snippet below.
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
    
## dce.py

This script is used to get dates/versions available and get individual file.

1. Get dates/versions available   
   ```
   python dce.py --run -env=<env> -key-path=<path to keys.json>
   ```
   - --run : Need to specify for actual execution. Otherwise, help is printed.
   - -key-path=<path to keys.json> : ../keys.json is used if not specified (optional) 
   - -env=<env> : stg or prod   
   
   Example:
     ```
     python dce.py --run -env=stg
     ```
     Expected:
     {"data":["/manifests/2016-07-25/v1/manifest.json","/manifests/2016-08-08/v1/manifest.json"]}
     
2. Get individual file
   ```
   python dce.py --run -env=<env> -path=<path> -dest=<dest> -key-path=<path to keys.json>
   ```
   - --run : Need to specify for actual execution. Otherwise, help is printed.
   - -key-path=<path to keys.json> : ../keys.json is used if not specified (optional) 
   - -env=<env> : stg or prod 
   - -path=<path> : path to file, which can be manifest.json or items found in manifest.json 
   - -dest=<dest> : target folder, which must be valid existing directory, to copy data to. Current folder '.' is used if not specified. (optional)
   
   Example:
     ```
     python dce.py --run -env=stg -path=/manifests/2016-08-22/v1/manifest.json -dest=.
     ```
     Expected:
     manifest.json is downloaded at current folder.
     
    
## dcebulk.py

- This script is used to get files in bulk.
  ```
  python dcebulk.py --run -env=<env> -manifest=<path of manifest.json> -dest=<dest>
  ```
  - --run : Need to specify for actual execution. Otherwise, help is printed
  - -key-path=<path to keys.json> : ../keys.json is used if not specified (optional) 
  - -env=<env> : stg or prod (must)
  - -manifest=<path of manifest.json> : path of manifest.json
  - -dest=<dest> : target folder, which must be valid existing directory, to copy data to. Current folder '.' is used if not specified. (optional)
  - -type=<type> : data type to download. If not specified, all data are downloaded. (optional)
 

  Example
    
  1. Get all data on specific date
    ```
    python dcebulk.py --run -env=stg -manifest=/manifests/2016-08-22/v1/manifest.json -dest=.
    ```
      - This will download all data of v1 on 2016-08-22 . Downloaded files can be found at <dest>/<type>, like ./answers/, ./questions/, ./reviews/
    
  2. Get 'reviews' on specific date
    ```
    python dcebulk.py --run -env=stg -manifest=/manifests/2016-08-22/v1/manifest.json -dest=. -type=reviews
    ```
      - This will download all data of v1 on 2016-08-22 . Downloaded files for reviews can be found at <dest>/reviews/