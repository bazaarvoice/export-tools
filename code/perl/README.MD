# DCE Tools in Perl

Two Perl scripts files are provided to get data from DCE service in BV. Please refer to https://developer.bazaarvoice.com/docs/displayable_content_export/ for details.

Please replace "x-api-key" and "shared secret" with real x-api-key key and shared secret key before running scripts.
```
   my %envs = (
    stg  => {xApiKey => 'x-api-key', secret => 'shared secret'},
    prod  => {xApiKey => 'x-api-key', secret => 'shared secret'}
  );
```

## Prerequisite
1. You should have perl v5.18.2 or later installed.
2. You should have SHA module for Perl installed.
3. You should have JSON module installed. (For mac os, running "brew install cpanm"  and "sudo cpanm install JSON")

## dcecurl.pl

This script is used to get dates/versions available and get individual file.

1. Get dates/versions available   
   ```
   /dceurl.pl --env=<env> 
   ```
   - --env=<env> : stg or prod   
   
   Example:
     ```
     ./dcecurl.pl --env=stg
     ```
     Expected:
     {"data":["/manifests/2016-07-25/v1/manifest.json","/manifests/2016-08-08/v1/manifest.json"]}
     
2. Get individual file
   ```
   ./dceurl.pl --env=<env> --path=<path> --dest=<dest>
   ```
   - --env=<env> : stg or prod 
   - --path=<path> : path to file, which can be found in manifest.json. 
   - --dest=<dest> : target folder to store data. Current folder '.' is used if not specified. (optional)
   
   Example:
     ```
     ./dcecurl.pl --env=stg --path=/manifests/2016-08-22/v1/manifest.json --dest=.
     ```
     Expected:
     manifest.json is downloaded at current folder.
     
    
## dcecurl_bulk.pl

- This script is used to get files in bulk.
  ```
  ./dcecurl_bulk.pl --env=<env> --date=<date> --version=<version> --dest=<dest>
  ```
  - --env=<env> : stg or prod (must)
  - --date=<date> : date like 2016-08-22 (must)
  - --version=<version> : version of data, like v1, v2, ... (must)
  - --dest=<dest> : target folder to store data. Current folder '.' is used if not specified. (optional)
  - --cat=<category> : category data to download. If not specified, all data are downloaded. (optional)
 

Example

1. Get all data on specific date
```
./dcecurl.pl --env=stg --date=2016-08-22 --version=v1 --dest=.
```
  - This will download all data of v1 on 2016-08-22 . Downloaded files can be found at <dest>/<category>, like ./answers/, ./questions/, ./reviews/

2. Get 'reviews' on specific date
```
./dcecurl.pl --env=stg --date=2016-08-22 --version=v1 --dest=. --cat="reviews"
```
  - This will download all data of v1 on 2016-08-22 . Downloaded files for reviews can be found at <dest>/reviews/