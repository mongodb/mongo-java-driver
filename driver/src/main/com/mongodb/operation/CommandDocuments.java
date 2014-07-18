/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.DocumentHelper.putIfTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class CommandDocuments {
    private CommandDocuments() {
    }

    static BsonDocument createMapReduce(final String collectionName, final MapReduce mapReduce) {

        BsonDocument commandDocument = new BsonDocument("mapreduce", new BsonString(collectionName))
                                       .append("map", asValueOrNull(mapReduce.getMapFunction()))
                                       .append("reduce", asValueOrNull(mapReduce.getReduceFunction()))
                                       .append("out", mapReduce.isInline() ? new BsonDocument("inline", new BsonInt32(1))
                                                                           : outputAsDocument(mapReduce.getOutput()))
                                       .append("query", asValueOrNull(mapReduce.getFilter()))
                                       .append("sort", asValueOrNull(mapReduce.getSortCriteria()))
                                       .append("finalize", asValueOrNull(mapReduce.getFinalizeFunction()))
                                       .append("scope", asValueOrNull(mapReduce.getScope()))
                                       .append("verbose", BsonBoolean.valueOf(mapReduce.isVerbose()));
        putIfNotZero(commandDocument, "limit", mapReduce.getLimit());
        putIfNotZero(commandDocument, "maxTimeMS", mapReduce.getMaxTime(MILLISECONDS));
        putIfTrue(commandDocument, "jsMode", mapReduce.isJsMode());
        return commandDocument;
    }

    private static BsonValue asValueOrNull(final BsonValue value) {
        return value == null ? BsonNull.VALUE : value;
    }

    private static BsonDocument outputAsDocument(final MapReduceOutputOptions output) {
        BsonDocument document = new BsonDocument(output.getAction().getValue(), new BsonString(output.getCollectionName()));
        document.append("sharded", BsonBoolean.valueOf(output.isSharded()));
        document.append("nonAtomic", BsonBoolean.valueOf(output.isNonAtomic()));
        if (output.getDatabaseName() != null) {
            document.put("db", new BsonString(output.getDatabaseName()));
        }
        return document;
    }
}
