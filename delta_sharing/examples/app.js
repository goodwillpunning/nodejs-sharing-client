/**
 * Copyright (C) 2022 The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const { SharingClient, parseUrl }  = require('../delta-sharing.js');
const { DeltaSharingProfile, Share, Schema, Table } = require('../protocol.js');
const { DeltaSharingReader } = require('../reader.js');
const { DataSharingRestClient } = require('../rest-client.js');

const profile = DeltaSharingProfile.readFromFile('./test/test-profile.json');
const client = new SharingClient(profile);
const restClient = new DataSharingRestClient(profile);

// List all shares
client.listSharesAsync().then(function(shares) {
  console.log('Listing shares...');
  shares.map(function(share) {
    console.log(share.toString());
  });
})
.catch(function(error) {
  console.log(error.toString());
});

// List all schemas
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

// List all tables in a schema
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

// List all tables available
client.listAllTablesAsync().then(function(tables) {
  console.log('Listing all tables...');
  tables.map(function(table) {
    console.log(table.toString());
  });
})
.catch(function(error) {
  console.log(error);
});

// Parse a sharing URL
var url = "<profile>#<share>.<schema>.<table>"
console.log(parseUrl(url));

// List files in table
const table = new Table('boston-housing', 'delta_sharing', 'default');
restClient.listFilesInTable(table).then(function(files) {
  console.log('Listing all files...');
  console.log(files.toString())  
})
.catch(function(error) {
  console.log(error);
});

// Query table metadata
restClient.queryTableMetadataAsync(table).then(function(metaData) {
  console.log('Listing table metaData...');
  console.log(metaData.toString())  
})
.catch(function(error) {
  console.log(error);
});

// Create a DataFrame
const reader = new DeltaSharingReader(table, restClient);
reader.createDataFrame().then(function(df) {
  console.log('Created DataFrame')
  df.print()
  console.log('Shape: ' + df.shape)
  console.log('Columns: ' + df.columns)
  console.log('Size: ' + df.size)
})
.catch(function(error) {
  console.log(error);
});
