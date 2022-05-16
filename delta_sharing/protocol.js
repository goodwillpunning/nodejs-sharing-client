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
const path = require('path');

class DeltaSharingProfile {
    static CURRENT = 1;

    constructor(shareCredentialsVersion, endpoint, bearerToken, expirationTime) {
        this.shareCredentialsVersion = shareCredentialsVersion;
        this.endpoint = endpoint;
        this.bearerToken = bearerToken;
        this.expirationTime = expirationTime;

        if (this.shareCredentialsVersion > DeltaSharingProfile.CURRENT) {
            throw "'shareCredentialsVersion' in the profile is " + this.share_credentials_version +
                " which is too new. The current release supports version " + DeltaSharingProfile.CURRENT +
                " and below. Please upgrade to a newer release."
        }
    }

    toString() {
        return 'DeltaSharingProfile(shareCredentialsVersion=' + this.shareCredentialsVersion +
               ', endpoint=' + this.endpoint + ', bearerToken=' + this.bearerToken + ', expirationTime=' +
               this.expirationTime + ')'
    }

    static readFromFile(profile) {
        var infile = null;
        if (typeof profile === 'string' || profile instanceof String) {
            infile = fs.readFileSync(path.resolve(__dirname, profile), 'utf8');
        } else {
            infile = profile;
        }
        return DeltaSharingProfile.fromJson(infile)
    }

    static fromJson(json) {
        var parsedJson = null
        if (typeof json === 'string' || json instanceof String || json instanceof ArrayBuffer) {
            parsedJson = JSON.parse(json);
        }
        var shareCredentialsVersion = parsedJson["shareCredentialsVersion"];
        var endpoint = parsedJson["endpoint"];
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.slice(0, -1)
        }
        var bearerToken = parsedJson["bearerToken"];
        var expirationTime = parsedJson["expirationTime"];
        return new DeltaSharingProfile(
            shareCredentialsVersion = shareCredentialsVersion,
            endpoint = endpoint,
            bearerToken = bearerToken,
            expirationTime = expirationTime,
        )
    }
}

class Share {
    constructor(shareName) {
        this.shareName = shareName;
    }

    static fromJson(json) {
        var parsedJson = null;
        if (typeof json === 'string' || json instanceof String || json instanceof ArrayBuffer) {
            parsedJson = JSON.parse(json);
        }
        return new Share(parsedJson["shareName"])
    }

    toString() {
        return 'Share(shareName=' + this.shareName + ')'
    }
}

class Schema {

    constructor(schemaName, share) {
        this.schemaName = schemaName;
        this.share = share;
    }

    static fromJson(json) {
        var parsedJson = null;
        if (typeof json === 'string' || json instanceof String || json instanceof ArrayBuffer) {
            parsedJson = JSON.parse(json);
        }
        return new Schema(parsedJson["schemaName"], parsedJson["share"])
    }

    toString() {
        return 'Schema(schemaName=' + this.schemaName + ', share=' + this.share + ')'
    }
}

class Table {
    constructor(tableName, share, schema) {
        this.tableName = tableName;
        this.share = share;
        this.schema = schema;
    }

    static fromJson(json) {
        var parsedJson = null;
        if (typeof json === 'string' || json instanceof String || json instanceof ArrayBuffer) {
            parsedJson = JSON.parse(json);
        }
        return new Table(parsedJson["tableName"], parsedJson["share"], parsedJson["schema"])
    }

    toString() {
        return 'Table(tableName=' + this.tableName + ', share=' + this.share + ', schema=' + this.schema + ')'
    }
}

class Protocol {
    static CURRENT = 1;

    constructor(minReaderVersion) {
        this.minReaderVersion = minReaderVersion;

        if (this.minReaderVersion > Protocol.CURRENT) {
            throw "The table requires a newer version " + this.minReaderVersion +
                " to read. But the current release supports version " + Protocol.CURRENT +
                " and below. Please upgrade to a newer release."
        }
    }

    static fromJson(json) {
        var parsedJson = null;
        if (typeof json === 'string' || json instanceof String || json instanceof ArrayBuffer) {
            parsedJson = JSON.parse(json);
        }
        return new Protocol(parsedJson["minReaderVersion"])
    }

    toString() {
        return 'Protocol(minReaderVersion=' + this.minReaderVersion + ')'
    }
}

class Format {
    constructor(provider = "parquet", options = {}) {
        this.provider = provider;
        this.options = options;
    }

    static fromJson(json) {
        var parsedJson = null;
        if (typeof json === 'string' || json instanceof String || json instanceof ArrayBuffer) {
            parsedJson = JSON.parse(json);
        }
        return new Format(parsedJson["provider"], parsedJson["options"])
    }

    toString() {
        return 'Format(provider=' + this.provider + ', options=' + this.options + ')'
    }
}

class Metadata {
    constructor(id, tableName = "", description = "", format = new Format(), schemaString = "", partitionColumns = []) {
        this.id = id;
        this.tableName = tableName;
        this.description = description;
        this.format = format;
        this.schemaString = schemaString;
        this.partitionColumns = partitionColumns;
    }

    static fromJson(json) {
        var parsedJson = null;
        if (typeof json === 'string' || json instanceof String || json instanceof ArrayBuffer) {
            parsedJson = JSON.parse(json);
        }
        return new Metadata(
            parsedJson["id"],
            parsedJson["tableName"],
            parsedJson["description"],
            Format.fromJson(JSON.stringify(parsedJson["format"])),
            parsedJson["schemaString"],
            parsedJson["partitionColumns"]
        )
    }

    toString() {
        return 'Metadata(\n' +
        '\tid=' + this.id + ',\n' +
        '\ttableName=' + this.tableName + ',\n' +
        '\tdescription=' + this.description + ',\n' +
        '\tformat=' + this.format + ',\n' +
        '\tpartitionColumns=' + this.partitionColumns + ',\n)'
     }
}

class AddFile {
    constructor(url, id, partitionValues, size, stats = null) {
        this.url = url;
        this.id = id;
        this.partitionValues = partitionValues;
        this.size = size;
        this.stats = stats;
    }

    static fromJson(json) {
        var parsedJson = null;
        if (typeof json === 'string' || json instanceof String || json instanceof ArrayBuffer) {
            parsedJson = JSON.parse(json);
        }
        return new AddFile(
            parsedJson["url"],
            parsedJson["id"],
            parsedJson["partitionValues"],
            parsedJson["size"],
            parsedJson["stats"]
        )
    }

    toString() {
        return 'AddFile(\n' +
        '\turl=' + this.url + ',\n' +
        '\tid=' + this.id + ',\n' +
        '\tpartitionValues=' + this.partitionValues + ',\n' +
        '\tsize=' + this.size + ',\n' +
        '\tstats=' + this.stats + '\n)'
    }
}

module.exports = {
    DeltaSharingProfile,
    Share,
    Schema,
    Table,
    Protocol,
    Format,
    Metadata,
    AddFile
}
