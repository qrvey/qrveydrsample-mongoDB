# Introduction
This sample shows how to use DataRouter to upload data into Qrvey platform. 

Qrvey DataRouter provides developers with a means for loading their data from a variety of different sources into Qrvey’s high performance, ElasticSearch backend. DataRouter provides an API that can be used for creating metadata data mapping and custom transformations, live update data push, and large volume historical data loads.   It is designed to scale for all types of data volume and velocity scenarios.

# Prerequisites
Before using this sample, please have the following values
1. Metadata URL. Contact help@qrvey.com if you don't have one.
2. Postdata URL. Contact help@qrvey.com if you don't have one.
3. Datarouter API Key
4. AWS Account credentials (AccessKey and SecretKey) where DataRouter is installed.
5. S3 bucket in AWS Account.

# Installation

1. Install NodeJS from https://nodejs.org (v12 recommended)
2. Run the command to load data 

# Steps

## 1. Create Metadata

Create the metadata specifying the fields and data types.

Example:

```
curl --location --request POST '{{metadataurl}}/v5/metadata?publicConnection=true' \
--header 'x-api-key: {{api-key}}' \
--header 'Content-Type: application/json' \
--data-raw '{
    "MetaDataId": "quick_start_index_name",
    "indexName": "quick_start_index_name",
    "skipOnPartialData": false,
    "dateFormat": "YYYY-MM-DDTHH:mm:ss",
    "columnType": [
        {
            "columnName": "CompanyId",
            "type": "numeric-general"
        },
        {
            "columnName": "Company Name",
            "type": "text-label"
        },
        {
            "columnName": "Foundation Date",
            "type": "date"
        }
    ]
}'
```

## 2. Create/Update a file called "config.json".

In root folder with the following JSON:
```
{
  "mongodburl": "",
  "mongodbcollection": "",
  "mongodbdatabase": "",
  "mongodbquery": {},
  "s3Bucket": "", //Bucket name created in AWS Account. See Prerequisites #5
  "metadataId": "",
  "postdataurl": "",
  "apikey": ""
}

```

# Usage

Load Data: `node index.js`