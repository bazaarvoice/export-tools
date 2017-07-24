# DCE Downloader Tools in Python

Two Python scripts files are provided to get data from the Bazaarvoice Displayable Content Export service. Please refer to [documentation](https://developer.bazaarvoice.com/displayable-content-export) for details.

## Prerequisite
1. You should have Python v2.7.10 or later installed.
2. You should have all dependencies installed (as listed in [requirements.txt](requirements.txt))
3. You must have a config.json file (see [Common Prerequisites](../README.MD#Common-Prerequisites))
    
## Development

To get some background on working with Python, check out [this blog post](http://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/) and [this virtualenv doc](http://docs.python-guide.org/en/latest/dev/virtualenvs/#virtualenvwrapper).

### MacOS Environment

In your terminal, execute the following:

```bash
# Install python
brew install python

# Install pip
sudo easy_install pip

# Install virtualenv and virtualenvwrapper
pip install virtualenv
pip install virtualenvwrapper

# Create a folder to store your virtualenvs
mkdir ~/.virtualenvs
```

Using your favorite text editor, add the following to your .bashrc (if you use something other than bash, adjust accordingly):

```bash
# Python Virtual Environment Settings
export WORKON_HOME=$HOME/.virtualenvs
if [ -f /usr/local/bin/virtualenvwrapper.sh ]; then
    source /usr/local/bin/virtualenvwrapper.sh;
fi;
```

Save and close your ~/.bashrc. Then execute the following:

```bash
# Create the project virtual environment
source ~/.bashrc
mkvirtualenv -p /usr/bin/python2.7 dce
workon dce

# From the dce-tools top-level directory, install the Python Requirements:
cd code/dce-downloader/python
pip install -r ./requirements.txt
add2virtualenv .
```

## dce.py

This script is used to get dates/versions available and get individual files. This demonstrates the basic usage of the Displayable Content Export, including how to create the HMAC signature, the HTTP headers to include, and the manifest list and manifest file contents.

Usage:

  ```
  dce.py --env <env> --dest <dest> --path <path> --config <config>

  <config>   : config file (optional, defaults to ../config.json) 
  <env>      : environment in config file to use (required)
  <path>     : path of file to download (optional, manifests is downloaded in the absence of path)
  <dest>     : local directory in which to save the downloaded data files (optional)
  ```

1. Get available command-line options

   ```
   dce.py
   ```
   
1. Get manifests (dates/versions) available

   ```
   dce.py --env <env> --config <config>
   ```
   
   Example:
   
   ```
   dce.py -env=stg
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
   dce.py --env <env> --path <path> --config <config>
   ```
   
   Example:
   
   ```
   dce.py --env stg --path/manifests/full/2017-06-01/v2/manifest.json
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
   dce.py --env <env> --path <path> --dest <dest> --config <config>
   ```

   Example:

   ```
   dce.py --env stg --path /manifests/full/2017-06-01/v2/manifest.json --dest output
   ```

   Example response:
   
   ```
   Downloading /manifests/full/2017-06-01/v2/manifest.json
   Saving as output/manifests/full/2017-06-01/v2/manifest.json
   ```
   
## dcebulk.py

This script is used to get files in bulk, for a given date and version.

Usage:

  ```
  dcebulk.py --env <env> --date <date> --dest <dest> --v <version> --type <type> --config <config> --fulls --incrementals

  <config>  : config file (defaults to ../config.json) 
  <env>     : environment in config file to use
  <date>    : date of data to fetch, in YYYY-mm-dd format, eg., 2017-07-04
  <type>    : type of data, eg., reviews, comments, products, authors, categories, configuration, 
                 review-parent-keys (defaults to all)
  <dest>    : local directory in which to save the downloaded data files
  <version> : version ID of data files (defaults to v2)
  ```
 

Examples
    
1. Get available command-line options

   ```
   dcebulk.py
   ```

1. Get all 'fulls' data on specific date

   ```
   dcebulk.py --env stg --date 2016-08-22 --dest output --fulls
   ```
    
   This will download all 'fulls' data of v1 on 2016-08-22 into local directory ```output```.
        
1. Get v2 'incrementals' of type 'reviews' on specific date
  
   ```
   dcebulk.py --env stg --date 2016-08-22 --dest test --version v2 --type reviews --incrementals
   ```
    
   This will download all 'incrementals' reviews of v2 on 2016-08-22 into local directory ```test```.
