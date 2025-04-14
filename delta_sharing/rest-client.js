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
'use strict';

const axios = require('axios');
const packageJson = require('./package.json');
const {
    Share,
    Schema,
    Table,
    Protocol,
    Metadata,
    AddFile
} = require('./protocol.js');

class ListSharesResponse {
    constructor(shares, nextPageToken) {
        this.shares = shares;
        this.nextPageToken = nextPageToken;
    }

    toString() {
        return 'ListSharesResponse(shares=[' + this.shares.toString() + '], nextPageToken=' + this.nextPageToken + ')';
    }
}

class ListSchemasResponse {
    constructor(schemas, nextPageToken) {
        this.schemas = schemas;
        this.nextPageToken = nextPageToken;
    }

    toString() {
        return 'ListSchemasResponse(schemas=[' + this.schemas.toString() +
                '], nextPageToken=' + this.nextPageToken + ')';
    }
}

class ListTablesResponse {
    constructor(tables, nextPageToken) {
        this.tables = tables;
        this.nextPageToken = nextPageToken;
    }

    toString() {
        return 'ListTablesResponse(tables=[' + this.tables.toString() +
                '], nextPageToken=' + this.nextPageToken + ')';
    }
}

class ListAllTablesResponse {
    constructor(tables, nextPageToken) {
        this.tables = tables;
        this.nextPageToken = nextPageToken;
    }

    toString() {
        return 'ListTablesResponse(tables=[' + this.tables.toString() +
                '], nextPageToken=' + this.nextPageToken + ')';
    }
}

class QueryTableMetadataResponse {
    constructor(protocol, metadata) {
        this.protocol = protocol;
        this.metadata = metadata;
    }

    toString() {
        return 'QueryTableMetadataResponse(protocol=' + this.protocol.toString() +
               ', metadata=' + this.metadata.toString() +')';
    }
}

class QueryTableVersionResponse {
    constructor(deltaTableVersion) {
        this.deltaTableVersion = deltaTableVersion;
    }

    toString() {
        return 'QueryTableVersionResponse(deltaTableVersion=' + this.deltaTableVersion.toString() + ')';
    }
}

class ListFilesInTableResponse {
    constructor(protocol, metadata, addFiles) {
        this.protocol = protocol;
        this.metadata = metadata;
        this.addFiles = addFiles;
    }

    toString() {
        return 'ListFilesInTableResponse(protocol=' + this.protocol.toString() +
               ', metadata=' + this.metadata.toString() + ', addFiles=' + this.addFiles.toString() + ')';
    }
}

class DataSharingRestClient {

    static USER_AGENT = DataSharingRestClient.clientUserAgent();

    // Use `headers` to pass in custom headers, "delta-sharing-capabilities", etc.
    constructor(profile, numRetries=10, headers={}) {
        this._profile = profile;
        this._numRetries = numRetries;
        this._session = axios.create({
            baseURL: profile.endpoint,
            timeout: 2000,
            headers: {
                "Authorization": "Bearer " + profile.bearerToken,
                "User-Agent": DataSharingRestClient.USER_AGENT,
                ...headers
            }
        });
    }

    listSharesAsync() {
        var data = {}
        return this._session.get('/shares', {params: data})
        .then(response => {
            var shares = [];
            if (response.data.hasOwnProperty('items')) {
                var items = response.data.items;
                shares  = items.map(function(item) {
                    return new Share(item.name);
                });
            }
            return new ListSharesResponse(shares, DataSharingRestClient.getOrElse(response.data, 'nextPageToken', null));
        })
        .catch(error => {
            console.warn(error.toString());
            return new ListSharesResponse([], null);
        });
    }

    listSchemasAsync(share, maxResults = null, pageToken = null) {
        var data = {}
        return this._session.get('/shares/' + share.shareName + '/schemas', {params: data})
        .then(response => {
            var schemas = [];
            if (response.data.hasOwnProperty('items')) {
                var items = response.data.items;
                schemas = items.map(function(schema) {
                    return new Schema(schema.name, schema.share);
                });
            }
            return new ListSchemasResponse(schemas, DataSharingRestClient.getOrElse(response.data, 'nextPageToken', null));
        })
        .catch(error => {
            console.warn(error.toString());
            return new ListSchemasResponse([], null);
        });
    }

    listTablesAsync(schema, maxResults = null, pageToken = null) {
        var data = {}
        return this._session.get('/shares/' + schema.share + '/schemas/' + schema.schemaName + '/tables', {params: data})
        .then((response) => {
            var tables = [];
            if (response.data.hasOwnProperty('items')) {
                var items = response.data.items;
                tables = items.map(function(table) {
                    return new Table(table.name, table.share, table.schema);
                });
            }
            return new ListTablesResponse(tables, DataSharingRestClient.getOrElse(response.data, 'nextPageToken', null));
        })
        .catch(error => {
            console.warn(error.toString());
            return new ListTablesResponse([], null);
        });
    }

    listAllTablesAsync(share, maxResults = null, pageToken = null) {
        var data = {}
        return this._session.get('/shares/' + share.shareName + '/all-tables', {params: data})
        .then(response => {
            var tables = [];
            if (response.data.hasOwnProperty('items')) {
                var items = response.data.items;
                tables = items.map(function(table) {
                    return new Table(table.name, table.share, table.schema);
                });
            }
            return new ListAllTablesResponse(tables, DataSharingRestClient.getOrElse(response.data, 'nextPageToken', null));
        })
        .catch(error => {
            throw error;
        });
    }

    queryTableMetadataAsync(table) {
        return this._session.get('/shares/' + table.share +'/schemas/' + table.schema + '/tables/' + table.tableName + '/metadata')
        .then(response => {
            var protocolJson, metaDataJson;
            var responseChunks = response.data.split(/\n/);
            for (var i=0; i < responseChunks.length; i++) {
                if (responseChunks[i].length > 0) {
                    var parsedJSON = JSON.parse(responseChunks[i]);
                    if (parsedJSON.hasOwnProperty("protocol")) {
                        protocolJson = parsedJSON["protocol"];
                    } else if (parsedJSON.hasOwnProperty("metaData")) {
                        metaDataJson = parsedJSON["metaData"];
                    }
                }
            }
            return new QueryTableMetadataResponse(
                Protocol.fromJson(JSON.stringify(protocolJson)),
                Metadata.fromJson(JSON.stringify(metaDataJson))
            );
        })
        .catch(error => {
            throw error;
        });
    }

    listFilesInTable(table, predicateHints = null, limitHint = null, jsonPredicateHints = null, version = null) {
        var data = {};
        if (predicateHints != null)
            data["predicateHints"] = predicateHints;
        if (limitHint != null)
            data["limitHint"] = limitHint;
        if (jsonPredicateHints != null)
            data["jsonPredicateHints"] = jsonPredicateHints;
        if (version != null)
            data["version"] = version;
        const api_url = '/shares/' + table.share +'/schemas/' + table.schema + '/tables/' + table.tableName + '/query'
        return this._session.post(api_url, {params: data})
        .then(response => {
            var protocolJson, metaDataJson, files = [];
            var responseChunks = response.data.split(/\n/);
            for (var i=0; i < responseChunks.length; i++) {
                if (responseChunks[i].length > 0) {
                    var parsedJSON = JSON.parse(responseChunks[i]);
                    if (parsedJSON.hasOwnProperty("protocol")) {
                        protocolJson = parsedJSON["protocol"];
                    } else if (parsedJSON.hasOwnProperty("metaData")) {
                        metaDataJson = parsedJSON["metaData"];
                    } else if (parsedJSON.hasOwnProperty("file")) {
                        var addFile = AddFile.fromJson(JSON.stringify(parsedJSON["file"]));
                        files.push(addFile);
                    }
                }
            }
            return new ListFilesInTableResponse(
                Protocol.fromJson(JSON.stringify(protocolJson)),
                Metadata.fromJson(JSON.stringify(metaDataJson)),
                files
            );
        })
        .catch(error => {
            throw error;
        });
    }

    static getOrElse(json, key, defaultValue) {
        if (json.hasOwnProperty(key))
            return json[key]
        else
            return defaultValue
    }

    static clientUserAgent() {
        try {
            return "Delta-Sharing-Nodejs/" + packageJson.version
        } catch (err) {
            console.warn("Unable to load version information for Delta Sharing because of error " + err)
            return "Delta-Sharing-Nodejs/<unknown>"
        }
    }

}

module.exports = {
    ListSharesResponse,
    ListSchemasResponse,
    ListTablesResponse,
    ListAllTablesResponse,
    QueryTableMetadataResponse,
    QueryTableVersionResponse,
    ListFilesInTableResponse,
    DataSharingRestClient
};
