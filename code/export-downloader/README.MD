# Export Downloader Tools

The example scripts contained herein are provided to get data from the Bazaarvoice Export APIs. Please refer to specific API [documentation](https://developer.bazaarvoice.com/products) for details.

## Reference Languages
[Python 2.7](python)

[Node.js](node)

## Common Prerequisites
For each of the reference tools, the following prerequisites apply:

1. You should have a file called "config.json" under "./exporter-downloader" path, which includes the code snippet below. Please replace "{EXPORT-PRODUCT-PASSKEY}" and "{EXPORT-PRODUCT-SECRET}" with the passkey and secret key strings provided by the Bazaarvoice team.

2. Modification is also needed for the url value. Depending on the Bazaarvoice export product of interest you should replace the following: 
    
    [Displayable Content Export](https://developer.bazaarvoice.com/displayable-content-export) path appears as staging > "https://stg.api.bazaarvoice.com/dce/v3/data", production > "https://api.bazaarvoice.com/dce/v3/data"

    [Product Sentiment Insights API](https://developer.bazaarvoice.com/product-sentiment-insights-api/home) path appears as staging > "https://stg.api.bazaarvoice.com/psi/v1/data", production > "https://api.bazaarvoice.com/psi/v1/data"
 

```
{
"stg":
    {
        "passkey": "your_passkey_here",
        "secret": "your_secret_key_here",
        "url" : "https://stg.api.bazaarvoice.com/{EXPORT-PRODUCT-SHORTNAME}/{EXPORT-PRODUCT-VERSION}/data"
    },
"prod":
    {
        "passkey": "your_passkey_here",
        "secret": "your_secret_key_here",
        "url" : "https://api.bazaarvoice.com/{EXPORT-PRODUCT-SHORTNAME}/{EXPORT-PRODUCT-VERSION}/data"
    }
}
```

