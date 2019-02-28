# Export Downloader Tools in Node.js

Two Node.js scripts files are provided to get data from the Bazaarvoice Export APIs . Please refer to the [documentation](https://developer.bazaarvoice.com/poducts) for details.

## Prerequisite
1. You should have Node.js 6.0 or later installed.
2. You should install the package dependencies
3. You must have a config.json file (see [Common Prerequisites](../README.MD#Common-Prerequisites))
    
## Development

To get some background on working with Node.js, check out [Setting up a Node development environment](https://developer.mozilla.org/en-US/docs/Learn/Server-side/Express_Nodejs/development_environment).

### MacOS Environment

It is recommended that you use nvm to install and manage different versions of node.js. 

Refer to [Installing Node.js Tutorial: Using nvm](https://nodesource.com/blog/installing-node-js-tutorial-using-nvm-on-mac-os-x-and-ubuntu) for installing on MacOS, particularly "Step 0: The Quick Guide".

It is recommended that you 'nvm install' and 'nvm use' the latest LTS version.

### Install Node Packages
```
cd code/export-downloader/node
npm install
```

## exporter.js

This script is used to get dates/versions available and get individual files. This demonstrates the basic usage of the Bazaarvoice Exporter tools, including how to create the HMAC signature, the HTTP headers to include, and the manifest list and manifest file contents.

Usage:

  ```
  exporter.js --env <env> --path <path> --dest <dest> --config <config>

  <config>   : config file (optional, defaults to ../config.json) 
  <env>      : environment in config file to use (required)
  <path>     : path of file to download (optional, manifests is downloaded in the absence of path)
  <dest>     : local directory in which to save the downloaded data files (optional)
  ```

1. Get available command-line options

   ```
   exporter.js
   ```
   
1. Get manifests (dates/versions) available

   ```
   exporter.js --env <env> --config <config>
   ```
   
   Example:
   
   ```
   exporter.js --env stg
   ```
     
   Example response:
   
   ```
   Retrieving manifests
   {
    "manifests": [
        {
            "fulls": [
                {
                    "date": "2017-05-23",
                    "path": "/manifests/full/2017-05-23/v2/manifest.json"
                },
                {
                    "date": "2017-05-25",
                    "path": "/manifests/full/2017-05-25/v2/manifest.json"
                },
                {
                    "date": "2017-05-30",
                    "path": "/manifests/full/2017-05-30/v2/manifest.json"
                },
                {
                    "date": "2017-06-01",
                    "path": "/manifests/full/2017-06-01/v2/manifest.json"
                },
                {
                    "date": "2017-06-06",
                    "path": "/manifests/full/2017-06-06/v2/manifest.json"
                },
                {
                    "date": "2017-06-08",
                    "path": "/manifests/full/2017-06-08/v2/manifest.json"
                },
                {
                    "date": "2017-06-13",
                    "path": "/manifests/full/2017-06-13/v2/manifest.json"
                },
                {
                    "date": "2017-06-15",
                    "path": "/manifests/full/2017-06-15/v2/manifest.json"
                }
            ],
            "version": "v2"
        }
      ]
   }
   ```
     
1. Get individual file (output to console)

   ```
   exporter.js --env <env> --path <path> --config <config>
   ```
   
   Example:
   
   ```
   exporter.js --env stg --path /manifests/full/2017-06-01/v2/manifest.json
   ```
     
   Example response:
   
   ```
	Downloading /manifests/full/2017-06-01/v2/manifest.json
	{
	    "authors": [
	        {
	            "path": "/contents/full/2017-06-01/v2/author/part-1-r-01859.gz",
	            "recordCount": 83117
	        }
	    ],
	    "categories": [
	        {
	            "path": "/contents/full/2017-06-01/v2/category/part-1-r-01859.gz",
	            "recordCount": 8
	        }
	    ],
	    "comments": [
	        {
	            "path": "/contents/full/2017-06-01/v2/comment/part-1-r-01859.gz",
	            "recordCount": 757
	        }
	    ],
	    "configuration": [
	        {
	            "path": "/contents/full/2017-06-01/v2/configuration/config.json",
	            "recordCount": 1
	        }
	    ],
	    "products": [
	        {
	            "path": "/contents/full/2017-06-01/v2/product/part-1-r-01859.gz",
	            "recordCount": 234
	        }
	    ],
	    "review-parent-keys": [
	        {
	            "path": "/contents/full/2017-06-01/v2/review-parent-keys/part-1-r-01859.gz",
	            "recordCount": 86508
	        }
	    ],
	    "reviews": [
	        {
	            "path": "/contents/full/2017-06-01/v2/review/part-1-r-01859.gz",
	            "recordCount": 42307
	        }
	    ]
	}
	```
     
1. Save individual file to local file. This can be used for individual manifest files as well as content files listed in a manifest.

   ```
   exporter.js --env <env> --path <path> --dest <dest> --config <config>
   ```

   Example:

   ```
   exporter.js --env stg --path /manifests/full/2017-06-01/v2/manifest.json --dest output
   ```

   Example response:
   
   ```
   Downloading /manifests/full/2017-06-01/v2/manifest.json
   Saving as output/manifests/full/2017-06-01/v2/manifest.json
   ```
   
## exporterbulk.js

This script is used to get files in bulk, for a given date and version.

Usage:

  ```
  exporterbulk.js --env <env> --date <date> --dest <dest> --v <version> --type <type> --config <config> --fulls --incrementals

  <config>  : config file (defaults to ../config.json) 
  <env>     : environment in config file to use (required)
  <date>    : date of data to fetch, in YYYY-mm-dd format, eg., 2017-07-04 (required)
  <type>    : type of data, eg., reviews, comments, products, authors, categories, configuration, 
                 review-parent-keys (defaults to all)
  <dest>    : local directory in which to save the downloaded data files (required)
  <version> : version ID of data files
  ```
 
Examples
    
1. Get available command-line options

   ```
   exporterbulk.js
   ```

1. Get all 'fulls' data on specific date

   ```
   exporterbulk.js --env stg --date 2016-08-22 --dest output --fulls
   ```
    
   This will download all 'fulls' data of v1 on 2016-08-22 into local directory ```output```.
        
1. Get v2 'incrementals' of type 'reviews' on specific date
  
   ```
   exporterbulk.js --env stg --date 2016-08-22 --dest test --version v2 --type reviews --incrementals
   ```
    
   This will download all 'incrementals' reviews of v2 on 2016-08-22 into local directory ```test```.
