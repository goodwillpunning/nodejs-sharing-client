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

const dfd = require('danfojs-node');
const parquet = require('parquetjs-lite');
const request = require('request');
const parse = require('url-parse');

class DeltaSharingReader {

    #table = null;
    #restClient = null;
    #predicateHints = null;
    #limit = null;
    #jsonPredicateHints = null;
    #version = null;

    constructor(table, restClient, predicateHints = null, limit = null, jsonPredicateHints = null, version = null){
        this.#table = table;
        this.#restClient = restClient;
        if (predicateHints != null) {
            this.#predicateHints = predicateHints;
        }
        if (limit != null) {
            this.#limit = limit;
        }
        if (jsonPredicateHints != null) {
            this.#jsonPredicateHints = jsonPredicateHints;
        }
        if (version != null) {
            this.#version = version;
        }
    }

    table() {
        return this.#table;
    }

    predicateHints(predicateHints) {
        return this.#copy(predicateHints, this.#limit);
    }

    limit(limit = null) {
        return this.#copy(this.#predicateHints, limit);
    }

    jsonPredicateHints(jsonPredicateHints = null) {
        return this.#copy(this.#predicateHints, this.#limit, jsonPredicateHints);
    }   

    version(version = null) {
        return this.#copy(this.#predicateHints, this.#limit, this.#jsonPredicateHints, version);
    }
    

    #copy(predicateHints = null, limit = null) {
        return new DeltaSharingReader(
            this.#table,
            this.#restClient,
            predicateHints,
            limit
        );
    }

    getEmptyDataFrame(schemaJson) {
        return new dfd.DataFrame()
    }

    async createDataFrame() {

        const response = await this.#restClient.listFilesInTable(
            this.#table, this.#predicateHints, this.#limit, this.#jsonPredicateHints, this.#version
        );

        const schemaJson = response.metadata.schemaString;

        // Return an empty DataFrame if no data present
        if (response.addFiles.length == 0 || this.#limit == 0) {
            return this.getEmptyDataFrame(schemaJson)
        }

        var dfPromises = []
        if (this.#limit == null) {
            for (const addFile of response.addFiles) {
                const dataPromise = this.#toArray(addFile, null, null)
                    .then(a => new dfd.DataFrame(a))
                dfPromises.push(dataPromise);
            }
            var finalDf = await Promise.all(dfPromises)
            .then(function(dfs) {
                return dfd.concat({ dfList: dfs, axis: 0 })
            })
            .catch(function(error) {
                console.log(error);
                throw error
            })

            return finalDf
        }
    }

    async #toArray(addFile, converters, limit) {
        const url = addFile.url;
        var protocol = parse(addFile.url, true).protocol

        let reader = await parquet.ParquetReader.openUrl(request, url)
        let cursor = reader.getCursor();
        let record = null;
        const json_rows = [];
        while (record = await cursor.next()) {
            json_rows.push(record);
        }
        await reader.close();

        return json_rows;
    }

    getEmptyArray(schemaJson) {
        return [];
    }

    async createDataArray() {
        const response = await this.#restClient.listFilesInTable(
            this.#table, this.#predicateHints, this.#limit
        );

        const schemaJson = response.metadata.schemaString;

        // Return an empty array if no data present
        if (response.addFiles.length === 0 || this.#limit == 0) {
            return this.getEmptyArray(schemaJson)
        }

        const dataPromises = []
        if (this.#limit == null) {
            for (const addFile of response.addFiles) {
                dataPromises.push(this.#toArray(addFile, null, null))
            }
            const results = await Promise.all(dataPromises);
            return results.flat();
        }
    }
}

module.exports = { DeltaSharingReader };
