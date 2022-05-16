/**
 * Copyright (C) 2021 The Delta Lake Project Authors.
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

const fs = require('fs');
const { DeltaSharingProfile, Share } = require('./protocol.js');
const { DataSharingRestClient } = require('./rest-client.js');
const dfd = require("danfojs-node");


/**
 *  Parses a shared dataset URL.
 * 
 *  :param url: a url under the format "<profile>#<share>.<schema>.<table>""
 *  :return: a tuple with parsed (profile, share, schema, table)
 *
 */
function parseUrl(url) {
    var shapeIndex = url.indexOf("#");
    if (shapeIndex < 0)
        throw "Invalid 'url': " + url;
    var profile = url.substring(0, shapeIndex);
    var fragments = url.substring(shapeIndex + 1).split(".");
    if (fragments.length != 3)
        throw "Invalid 'url': " + url;
    let share, schema, table;
    [share, schema, table] = fragments;
    if (profile.length == 0 || share.length == 0 || schema.length == 0 || table.length == 0)
        throw "Invalid 'url': " + url;
    return [profile, share, schema, table]
}

/**
  * Load the shared table using the give url as a pandas DataFrame.
  *
  * :param url: a url under the format "<profile>#<share>.<schema>.<table>"
  * :param limit: a non-negative int. Load only the ``limit`` rows if the parameter is specified.
  *   Use this optional parameter to explore the shared table without loading the entire table to
  *   the memory.
  * :return: A pandas DataFrame representing the shared table.
 */
function loadAsPandas(url, limit = null) {
    profile_json, share, schema, table = _parse_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    return DeltaSharingReader(
        table=Table(table, share, schema),
        rest_client=DataSharingRestClient(profile),
        limit=limit,
    ).to_pandas()
}


/**
 * A Delta Sharing client to query shares/schemas/tables from a Delta Sharing Server.
 */
class SharingClient {

    constructor(profile) {
        if (!(profile instanceof DeltaSharingProfile)) {
            profile = DeltaSharingProfile.readFromFile(profile)
        }
        this._profile = profile
        this._restClient = new DataSharingRestClient(profile)
    }

    async #listAllTablesInShare(share) {
        var tables = []
        var pageToken = null
        while (true) {
            var response = await this._restClient.listAllTablesAsync(share, pageToken);
            tables = tables.concat(response.tables)
            var pageToken = response.nextPageToken
            if (pageToken == null) {
                return tables
            }
        }
    }

    /**
     * List shares that can be accessed by you in a Delta Sharing Server.
     *
     * @return {Share} the shares that can be accessed.
     */
    async listSharesAsync() {
        var shares = []
        var pageToken = null
        while (true) {
            var response = await this._restClient.listSharesAsync(pageToken);
            shares = shares.concat(response.shares)
            pageToken = response.nextPageToken
            if (pageToken == null) {
                return shares
            }
        }
    }

    /**
     * List schemas in a share that can be accessed by you in a Delta Sharing Server.
     *
     * @param share {Share} the share to list.
     * @return {Array} the schemas in a share.
     */
    async listSchemasAsync(share) {
        var schemas = []
        var pageToken = null
        while (true) {
            var response = await this._restClient.listSchemasAsync(share, pageToken)
            schemas = schemas.concat(response.schemas)
            pageToken = response.nextPageToken
            if (pageToken == null) {
                return schemas
            }
        }
    }

    /**
     * List tables in a schema that can be accessed by you in a Delta Sharing Server.
     *
     * @param schema {String} the schema to list.
     * @return {Array} the tables in a schema.
     */
    async listTablesAsync(schema) {
        var tables = []
        var pageToken = null
        while (true) {
            var response = await this._restClient.listTablesAsync(schema, pageToken)
            tables = tables.concat(response.tables)
            pageToken = response.nextPageToken
            if (pageToken == null) {
                return tables
            }
        }
    }

    /**
     * List all tables that can be accessed by you in a Delta Sharing Server.
     *
     * @return {Array} all tables that can be accessed.
     */
    async listAllTablesAsync() {
        const shares = await this.listSharesAsync();
        var allTables = []
        try {
            for(const share of shares) {
                const tablesInShare = await this.#listAllTablesInShare(share);
                allTables = allTables.concat(tablesInShare);
            }
            return allTables;
        } catch (error) {
            const cleansedErrorMessage = JSON.stringify(error).replace('\\n', '');
            const parsedErrorJson = JSON.parse(cleansedErrorMessage);

            if (parsedErrorJson['status'] == 404) {
                // The server doesn't support all-tables API. Fallback to the old APIs instead.
                console.warn('The Delta Sharing Server does not support all-tables API calls.');
                var allSchemas = [];
                for(const share of shares) {
                    var schemas = await this.listSchemasAsync(share);
                    allSchemas = allSchemas.concat(schemas);
                }
                for(const schema of allSchemas) {
                    var tablesInShare = await this.listTablesAsync(schema);
                    allTables = allTables.concat(tablesInShare);
                }
                return allTables;
            } else {
                throw error;
            }
        }
    }
}

module.exports = { SharingClient, parseUrl };
