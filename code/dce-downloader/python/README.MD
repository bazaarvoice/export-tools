# DCE Tools in Perl

Two Python scripts files are provided to get data from DCE service in BV. Please refer to https://developer.bazaarvoice.com/docs/displayable_content_export/ for details.

Please replace "x-api-key" and "shared secret" with real x-api-key key and shared secret key before running scripts.
```
DCE_KEYS = {
    "stg": {"x-api-key": "x-api-key", "secret": "shared secret"},
    "prod": {"x-api-key": "x-api-key", "secret": "shared secret"}
}
```

## Prerequisite
1. You should have Python v2.7.10 or later installed.

## dcecurl.py

This script is used to get dates/versions available and get individual file.

1. Get dates/versions available   
   ```
   python dceurl.py --run -env=<env> 
   ```
   - --run : Need to specify for actual execution. Otherwise, help is printed.
   - -env=<env> : stg or prod   
   
   Example:
     ```
     python dceurl.py --run -env=stg 
     ```
     Expected:
     {"data":["/manifests/2016-07-25/v1/manifest.json","/manifests/2016-08-08/v1/manifest.json"]}
     
2. Get individual file
   ```
   python dceurl.py --run -env=<env> -path=<path> -dest=<dest>
   ```
   - --run : Need to specify for actual execution. Otherwise, help is printed.
   - -env=<env> : stg or prod 
   - -path=<path> : path to file, which can be manifest.json or items found in manifest.json 
   - -dest=<dest> : target folder to store data. Current folder '.' is used if not specified. (optional)
   
   Example:
     ```
     python dceurl.py --run -env=stg -path=/manifests/2016-08-22/v1/manifest.json -dest=.
     ```
     Expected:
     manifest.json is downloaded at current folder.
     
    
## dcecurl_bulk.py

- This script is used to get files in bulk.
  ```
  python dceurl_bulk.pl --run -env=<env> -date=<date> -version=<version> -dest=<dest>
  ```
  - --run : Need to specify for actual execution. Otherwise, help is printed
  - -env=<env> : stg or prod (must)
  - -date=<date> : date like 2016-08-22 (must)
  - -version=<version> : version of data, like v1, v2, ... (must)
  - -dest=<dest> : target folder to store data. Current folder '.' is used if not specified. (optional)
  - -cat=<category> : category data to download. If not specified, all data are downloaded. (optional)
 

Example

1. Get all data on specific date
```
python dceurl_bulk.pl --run -env=stg -date=2016-08-22 -version=v1 -dest=.
```
  - This will download all data of v1 on 2016-08-22 . Downloaded files can be found at <dest>/<category>, like ./answers/, ./questions/, ./reviews/

2. Get 'reviews' on specific date
```
python dceurl_bulk.pl --run -env=stg -date=2016-08-22 -version=v1 -dest=. -cat="reviews"
```
  - This will download all data of v1 on 2016-08-22 . Downloaded files for reviews can be found at <dest>/reviews/