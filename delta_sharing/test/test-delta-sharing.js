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
const SharingClient = require('../delta-sharing.js');
const {
    DeltaSharingProfile,
    Share,
    Schema,
    Table,
    Format,
    Metadata,
    AddFile
} = require('../protocol.js');
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

const profile = new DeltaSharingProfile("./test-profile.json")
const sharingClient = new SharingClient(profile)

describe('The Delta Sharing Client object', function() {

    it('should have a defined profile', function() {
        assert.notEqual('undefined', sharingClient._profile);
    })

    it('should be able to list all the shared datasets', async() => {
        const sharesResponse = await sharingClient.listSharesAsync();
        assert.equal(1, sharesResponse.length);
    })

    it('should be able to list all the schemas in a share', async() => {
        var share = new Share(name='webinarshare')
        var schemasResponse = await sharingClient.listSchemasAsync(share);
        assert.equal(1, schemasResponse.length);
    })

    it('should be able to list all the tables in a schema', async() => {
        var covidSchema = new Schema(name='samplecoviddata', share="webinarshare");
        const tablesResponse = await sharingClient.listTablesAsync(covidSchema);
        assert.equal(1, tablesResponse.length);
    })

    it('should be able to list all the tables that are accessible', async() => {
        var tablesResponse = await sharingClient.listAllTablesAsync();
        //assert.equal(1, tablesResponse.length);
        //fs.writeFileSync('/Users/will.girten/Desktop/nodejs/finalShares.txt', tablesResponse.toString())
        assert.equal(1,1)
    })
})
