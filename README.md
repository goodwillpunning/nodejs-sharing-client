# Node.js Delta Sharing Connector
 
![Node.js Delta Sharing Connector Logo](/assets/images/delta-sharing-nodejs-transparent-bg.png)

The Node.js Delta Sharing connector allows you to load shared datasets in Node.js as DataFrames using the popular library, [Danfo.js](https://danfo.jsdata.org/).

_Please note that this project is currently a Beta version and is experimental in nature._

[Delta Sharing](https://delta.io/sharing) is an open protocol for secure real-time exchange of large datasets, which enables secure data sharing across different computing platforms. It lets organizations share access to existing [Delta Lake](https://delta.io) and [Apache Parquet](https://parquet.apache.org) tables with other organizations, who can then directly read the table in Pandas, Apache Spark, or any other software that implements the open protocol.
 
## System Requirements
 
The Node.js Delta Sharing connector requires **Node.js v16.9.0+**.
 
## Installation
 
You can download and install the Node.js connector using Node Package Manager:
 
```JavaScript
npm install delta-sharing
```
 
## Quickstart
 
### Step 1: Create a Sharing Profile
 
Before you can begin exploring shared datasets, you will need to create a sharing profile. A sharing profile contains important information about the sharing server for the connector, such as the URL of the sharing server, the sharing profile version number, and, in most cases, the access token.
For example, the following sharing profile contains information for connecting to the public sharing server hosted by Databricks. The sample sharing profile can be found under `delta_sharing/examples` folder and can be downloaded and used to follow along with the Quickstart.
 
```JSON
{
 "shareCredentialsVersion": 1,
 "endpoint": "https://sharing.delta.io/delta-sharing/",
 "bearerToken": "faaie590d541265bcab1f2de9813274bf233"
}
```
 
### Step 2: Create a Sharing Client
 
Next, import the Delta Sharing Client class and create a Sharing Client. The Sharing Client will expect the file location to the Sharing Profile as an argument.
 
```JavaScript
const {
  SharingClient,
  DeltaSharingProfile,
  Share,
  Schema,
  Table,
  DeltaSharingReader,
  DataSharingRestClient
} = require('delta-sharing');
 
// Specify the location to the Sharing Profile above as a local file
const sharingProfile = DeltaSharingProfile.readFromFile('./examples/sample-profile.share');

// ...Or the sharing profile can be expressed as a JSON String
const jsonString = `
{
    "shareCredentialsVersion": 1,
    "endpoint": "https://sharing.delta.io/delta-sharing/",
    "bearerToken": "faaie590d541265bcab1f2de9813274bf233"
}
`;
const sharingProfileFromJSON = DeltaSharingProfile.fromJson(jsonString);
 
// Create a Sharing Client to interact with the Sharing Server
const client = new SharingClient(sharingProfile);
```
 
### Step 3: Explore Contents of the Share
 
Now you are ready to begin exploring the secure datasets! A `Share` is a logical container that can contain one more schemas, with one or more tables. Let's start with listing all the available Shares.
 
```JavaScript
// List all available shares
client.listSharesAsync().then(function(shares) {
 console.log('Listing shares...');
 shares.map(function(share) {
   console.log(share.toString());
 });
})
.catch(function(error) {
 console.log(error.toString());
});
```
 
### Step 4: Explore the Schemas within a Share
 
Now that we have a general understanding of these logical containers known as Shares, we can explore the contents of the Shares. For example, shared datasets are expressed using a hierarchical structure comprised of the Share name, a `#`, followed by the fully-qualified table name:
 
```JavaScript
<share_name>#<schema_name>.<table_name>
```
 
Let's take a look at the Schemas contained within a particular Share by asking our Sharing client to list them:
 
```JavaScript
// List all schemas under a particular Share
const share = new Share('delta_sharing');
client.listSchemasAsync(share).then(function(schemas) {
 console.log('Listing schemas...');
 schemas.map(function(schema) {
   console.log(schema.toString());
 });
})
.catch(function(error) {
 console.log(error.toString());
});
```
 
### Step 5: Explore the Tables within a Schema
 
Similarly, we can explore all of the tables contained within a Schema by asking our Sharing client to list them:
 
```JavaScript
// List all tables in a particular Schema
const schema = new Schema('default', 'delta_sharing');
client.listTablesAsync(schema).then(function(tables) {
  console.log('Listing tables in schema...');
  tables.map(function(table) {
    console.log(table.toString());
  });
})
.catch(function(error) {
  console.log(error.toString());
});
```
 
### Step 6: Explore the Shared Dataset Metadata
 
We can also interact with the Sharing Client to display the Metadata about the shared dataset. The Metadata will contain information about the Delta table, like partitioning information, table name and identifier, the table description, and the table schema.
 
```JavaScript
// Query table metadata
const table = new Table('boston-housing', 'delta_sharing', 'default');
restClient.queryTableMetadataAsync(table).then(function(metaData) {
 console.log('Listing table metaData...');
 console.log(metaData.toString()) 
})
.catch(function(error) {
 console.log(error);
});
```
 
### Step 7: Loading the Shared Datasets as a DataFrame
 
You're now ready to begin loading the shared datasets and interacting with the data as a DataFrame using Danfo.js!
 
Let's begin by loading the sample dataset and performing simple  DataFrame operations like shape, columns, and size.
 
```JavaScript
// Display simple characteristics of a DataFrame
const reader = new DeltaSharingReader(table, restClient);
reader.createDataFrame().then(function(df) {
  console.log('Created DataFrame')
  // display contents of the DataFrame
  df.print()
  console.log('Shape: ' + df.shape)
  console.log('Columns: ' + df.columns)
  console.log('Size: ' + df.size)
})
.catch(function(error) {
  console.log(error);
});
```

### Step 8: Enjoy!

I hope this short tutorial was fun and has inspired you to use the Node.js Delta Sharing connector in your next Node.js application.

## Running the Sample Node.js Application

Alternatively, a sample Node.js application can be found at the `/delta_sharing/examples/app.js` which includes all of the examples above. Simply link the Node.js package, install the dependencies, and execute the `app.js` file in your Node.js runtime environment. 

```bash
cd delta_sharing
npm link
cd ../examples
npm link install delta-sharing
node app.js
```

## Additional Documentation
 
For more information about the Delta Sharing project, including information about the protocol and the Sharing Server REST API, please see the [the protocol documentation](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md) for additional details.

# Reporting Issues

Found a bug or issue in the code? Please open a new issue under the [GitHub Issues](https://github.com/goodwillpunning/nodejs-sharing-client/issues) so we can track and fix community reported issues.

# Contributing 
Come join our team of contributors! We absolutely welcome all code contributions to Node.js Delta Sharing connector. Please see the [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

# License
The Node.js Delta Sharing connector is free to download and use under the [Apache License 2.0](LICENSE.txt).
 