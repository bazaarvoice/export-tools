# DCE Downloader Tools

The example scripts contained herein are provided to get data from the Bazaarvoice Displayable Content Export service. Please refer to [documentation](https://developer.bazaarvoice.com/displayable-content-export) for details.

## Reference Languages
[Python 2.7](python)

[Node.js](node)

## Common Prerequisites
For each of the reference tools, the following prerequisites apply:

1. You should have a file called "config.json" under "./dce-downloader", which includes code snippet below. Please replace "your\_passkey\_here" and "your\_secret\_key_here" with the passkey and secret key strings provided by the Bazaarvoice team.
 
```
{
"stg":
    {
        "passkey": "your_passkey_here",
        "secret": "your_secret_key_here",
        "url" : "https://stg.api.bazaarvoice.com/dce/v3/data"
    },
"prod":
    {
        "passkey": "your_passkey_here",
        "secret": "your_secret_key_here",
        "url" : "https://api.bazaarvoice.com/dce/v3/data"
    }
}
```

