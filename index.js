const MongoClient = require("mongodb").MongoClient;
const AWS = require("aws-sdk");
const fs = require("fs");
const uuidv1 = require("uuid");
const https = require("https");
const s3 = new AWS.S3();

const config = require("./config.json");

//Use this code if your mongoDB use a certificate to connect
/*
const certFileBuf = fs.readFileSync('./ca.pfx');

const options = {
    sslCA: certFileBuf,
    useUnifiedTopology: true
}*/

//MongoDB Configuration
const mongodburl = config.mongodburl;
const mongodbcollection = config.mongodbcollection;
const mongodbdatabase = config.mongodbdatabase;
const mongodbquery = config.mongodbquery;

//Qrvey Configuration
const s3Bucket = config.s3Bucket;
const metadataId = config.metadataId;
const qrveyPostdataUrl = config.postdataurl;
const qrveyApiKey = config.apikey;

//Init dataloading
const initLoading = async () => {
  return new Promise((resolve, reject) => {
    const data = {
      datasetId: `ds_${collection}`,
      metadataId: metadataId,
      datasources: [
        {
          datasourceId: `dsource_${collection}`,
          indexName: metadataId,
          dataConnection: {
            appid: `appid-${collection}`,
            connectorid: `connector-${collection}`,
            connectorType: "FILE_UPLOAD",
            name: "JSON File Connector",
            s3Bucket: dataBucket,
            s3Path: folderName,
            contentType: "application/json",
          },
        },
      ],
    };
    var options = {
      hostname: qrveyPostdataUrl,
      path: "/Prod/dataload/init",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": qrveyApiKey,
      },
      method: "POST",
      port: 443,
    };
    var req = https.request(options, function (res) {
      var chunks = [];
      res.on("data", (chunk) => {
        chunks.push(chunk.toString());
      });
      res.on("end", () => {
        return resolve(chunks);
      });
      res.on("error", (error) => {
        return reject(error);
      });
    });
    var postData = JSON.stringify(data);
    req.write(postData);
    req.end();
  });
};

//Connect to mongoDB database
const connectToMongoDB = async () => {
  return new Promise(async (resolve, reject) => {
    try {
      const client = await MongoClient.connect(mongodburl, options);
      const adminDb = client.db(mongodbdatabase);
      const cursor = adminDb.collection(mongodbcollection);
      const stream = cursor.find(mongodbquery).stream();
      var batch = [];

      stream.on("end", async () => {
        if (batch.length > 0) {
          await loadDataToS3(batch);
        }
        console.log("Stream ended!");
        client.close();
        return resolve(true);
      });

      stream.on("data", async (doc) => {
        batch.push(doc);

        if (batch.length == 1000) {
          stream.pause();
          await loadDataToS3(batch);
          batch = [];
          stream.resume();
        }
      });
      stream.on("error", (error) => {
        client.close();
        return reject(error);
      });
    } catch (error) {
      client.close();
      return reject(error);
    }
  });
};

//Load data to S3 bucket
const loadDataToS3 = async (data) => {
  const params = {
    Body: JSON.stringify(data),
    Bucket: s3Bucket,
    ContentType: "application/json",
    Key: `${collection}/${uuidv1.v1()}.json`,
  };
  await s3.upload(params).promise();
};

const run = async () => {
  try {
    await connectToMongoDB();
    const jobId = await initLoading();
    console.log("jobId: ", jobId);
  } catch (error) {
    console.log("error: ", error);
  }
};

run();
