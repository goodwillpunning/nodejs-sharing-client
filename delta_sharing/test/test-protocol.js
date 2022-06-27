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

describe('The Delta Sharing Profile object', function() {
    it('should be able to correctly parse a JSON sharing profile', function() {
        var json = `
            {
                "shareCredentialsVersion": 1,
                "endpoint": "https://localhost/delta-sharing/",
                "bearerToken": "token"
            }
            `;
        profile = DeltaSharingProfile.fromJson(json);
        assert.deepEqual(profile, new DeltaSharingProfile(
            shareCredentialsVersion = 1,
            endpoint = "https://localhost/delta-sharing",
            bearerToken = "token"
        ));

        json = `
            {
                "shareCredentialsVersion": 1,
                "endpoint": "https://localhost/delta-sharing/",
                "bearerToken": "token",
                "expirationTime": "2021-11-12T00:12:29.0Z"
            }
            `;
        profile = DeltaSharingProfile.fromJson(json);
        assert.deepEqual(profile, new DeltaSharingProfile(
            shareCredentialsVersion = 1,
            endpoint = "https://localhost/delta-sharing",
            bearerToken = "token",
            expirationTime = "2021-11-12T00:12:29.0Z"
        ));
    })

    it('should be able to correctly parse a file-based sharing profile', function() {
        var profilePath = './test/test-profile.json';
        var profile = DeltaSharingProfile.readFromFile(profilePath);
        assert.deepEqual(profile, new DeltaSharingProfile(
            shareCredentialsVersion = 1,
            endpoint = "https://localhost:12345/delta-sharing",
            bearerToken = "dapi5e3574ec767ca1548ae5bbed1a2dc04d",
            expirationTime = "2021-11-12T00:12:29.0Z"
        ));

        profilePath = String('./test/test-profile.json');
        profile = DeltaSharingProfile.readFromFile(profilePath);
        assert.deepEqual(profile, new DeltaSharingProfile(
            shareCredentialsVersion = 1,
            endpoint = "https://localhost:12345/delta-sharing",
            bearerToken = "dapi5e3574ec767ca1548ae5bbed1a2dc04d",
            expirationTime = "2021-11-12T00:12:29.0Z"
        ));
    })

    it('should throw an error when an unsupported shareCredentialVersion is detected', function() {
        assert.throws(function() {
            var profile = new DeltaSharingProfile(
                shareCredentialsVersion = 999,
                endpoint = "http://localhost:12345/delta-sharing",
                bearerToken = "token"
            );
        });
    })
})

describe('The Share object', function() {
    it('should be correctly instantiated from raw JSON', function() {
        var json = `
            {
                "name": "share_name"
            }
            `;
        var share = Share.fromJson(json);
        assert.deepEqual(share, new Share(name = "share_name"));
    })
})

describe('The Schema object', function() {
    it('should be correctly instantiated from raw JSON', function() {
        var json = `
            {
                "name" : "schema_name",
                "share" : "share_name"
            }
            `;
        var schema = Schema.fromJson(json)
        assert.deepEqual(schema, new Schema("schema_name", "share_name"));
    })
})

describe('The Table object', function() {
    it('should be correctly instantiated from raw JSON', function() {
        var json = `
            {
                "name" : "table_name",
                "share" : "share_name",
                "schema" : "schema_name"
            }
            `;
        var table = Table.fromJson(json)
        assert.deepEqual(table, new Table("table_name", "share_name", "schema_name"));
    })
})

describe('The Protocol object', function() {
    it('should be correctly instantiated from raw JSON', function() {
        var json = `
            {
                "minReaderVersion" : 1
            }
            `;
        var protocol = Protocol.fromJson(json);
        assert.deepEqual(protocol, new Protocol(minReaderVersion = 1))
    })

    it('should throw an error when an unsupported shareCredentialVersion is detected', function() {
        assert.throws(function() {
            var json = `
            {
                "minReaderVersion" : 100
            }
            `;
            new Protocol.fromJson(json);
        })
    })
})

describe('The Format object', function() {
    it('should be correctly instantiated from raw JSON', function() {
        var json = `
            {
                "provider": "parquet",
                "options": {}
            }
            `;
        var format = Format.fromJson(json);
        assert.deepEqual(format, new Format(provider = "parquet", options = {}))
    })
})

describe('The Metadata object', function() {
    it('should be correctly instantiated from raw JSON', function() {
        var json = `
            {
                "id" : "testId",
                "name" : "test",
                "description" : "test",
                "format" : {
                    "provider" : "parquet",
                    "options" : {}
                },
                "schemaString" : "{}",
                "partitionColumns" : []
            }
            `;
        var metadata = Metadata.fromJson(json);
        assert.deepEqual(metadata, new Metadata(
            "testId",
            "test",
            "test",
            new Format(),
            schema_string = "{}",
            partition_columns = []
        ));
    })
})
