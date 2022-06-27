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

const assert = require('assert');
const fs = require('fs');
const {
    ListSharesResponse,
    ListSchemasResponse,
    ListTablesResponse,
    ListAllTablesResponse,
    QueryTableMetadataResponse,
    QueryTableVersionResponse,
    ListFilesInTableResponse,
    DataSharingRestClient
} = require('../rest-client.js');
const {
    DeltaSharingProfile,
    Share,
    Schema,
    Table,
    Protocol,
    Format,
    Metadata,
    AddFile
} = require('../protocol.js');
const json = `
    {
        "shareCredentialsVersion": 1,
        "endpoint": "http://localhost:8080/delta-sharing/",
        "bearerToken": "Or7V14qW"
    }
    `;
const sharingProfile = DeltaSharingProfile.fromJson(json);
const restClient = new DataSharingRestClient(sharingProfile);

describe('The Data Sharing Rest Client object', function() {
    it('should be able to parse the endpoint correctly', function() {
        assert.equal(false, restClient._profile.endpoint.endsWith("/"));
    })
})

describe('The Data Sharing Rest Client object', function() {
    it('should be able to list shares', async() => {
        var expectedShares = [
             new Share(name="webinarshare")
         ];
         const sharesResponse = await restClient.listSharesAsync();
         assert.deepEqual(sharesResponse, new ListSharesResponse(expectedShares, null));
    })
})

describe('The ListSharesResponse object', function() {
    it('should be able to convert to a String object', async() => {
        var expectedShares = [
             new Share(name="webinarshare")
         ];
         const sharesResponse = await restClient.listSharesAsync();
         assert.equal(sharesResponse.toString(), 'ListSharesResponse(shares=[Share(name=webinarshare)], nextPageToken=null)');
    })
})

describe('The Data Sharing Rest Client object', function() {
    it('should be able to list all schemas in a share', async() => {
        var expectedSchemas = [
             new Schema(name="samplecoviddata", share="webinarshare")
         ];
         var share = new Share(name="webinarshare");
         const schemasResponse = await restClient.listSchemasAsync(share);
         assert.deepEqual(schemasResponse, new ListSchemasResponse(expectedSchemas, null));
    })
})

describe('The ListSchemasResponse object', function() {
    it('should be able to convert to a String object', async() => {
         var share = new Share(name="webinarshare");
         const schemasResponse = await restClient.listSchemasAsync(share);
         assert.equal(schemasResponse.toString(), 'ListSchemasResponse(schemas=[Schema(name=samplecoviddata, share=webinarshare)], nextPageToken=null)');
    })
})

describe('The Data Sharing Rest Client object', function() {
    it('should be able to list tables in a given schema', async() => {
        const covidSchema = new Schema(name="samplecoviddata", share="webinarshare");
        var expectedTables = [
             new Table(name="time", share="webinarshare", schema="samplecoviddata"),
             new Table(name="timeage", share="webinarshare", schema="samplecoviddata")
        ];
        const tablesResponse = await restClient.listTablesAsync(covidSchema);
        assert.deepEqual(tablesResponse, new ListTablesResponse(expectedTables, null));
    })
})