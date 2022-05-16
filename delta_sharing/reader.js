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

const dfd = require('danfojs-node');
const parquet = require('parquetjs-lite');
const request = require('request');
const parse = require('url-parse');

class DeltaSharingReader {

    #table = null;
    #restClient = null;
    #predicateHints = null;
    #limit = null;

    constructor(table, restClient, predicateHints = null, limit = null){
        this.#table = table;
        this.#restClient = restClient;
        if (predicateHints != null) {
            this.#predicateHints = predicateHints;
        }
        if (limit != null) {
            this.#limit = limit;
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

    #copy(predicateHints = null, limit = null) {
        return new DeltaSharingReader(
            this.#table,
            this.#restClient,
            predicateHints,
            limit
        );
    }

    getEmptyTable(schemaJson) {
        return new dfd.DataFrame()
    }

    async createDataFrame() {

        const response = await this.#restClient.listFilesInTable(
            this.#table, this.#predicateHints, this.#limit
        );

        const schemaJson = response.metadata.schemaString;

        // Return an empty DataFrame if no data present
        if (response.addFiles.length == 0 || this.#limit == 0) {
            return this.getEmptyTable(schemaJson)
        }

        var dfPromises = []
        if (this.#limit == null) {
            for (const addFile of response.addFiles) {
                dfPromises.push(this.#toDF(addFile, null, null))
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

    async #toDF(addFile, converters, limit) {

        const url = addFile.url
        var protocol = parse(addFile.url, true).protocol

        let reader = await parquet.ParquetReader.openUrl(request, url)
        let cursor = reader.getCursor();
        let record = null;
        var json_rows = [];
        while (record = await cursor.next()) {
            json_rows.push(record);
        }
        reader.close();

        return new dfd.DataFrame(json_rows)

    }
}

module.exports = { DeltaSharingReader };