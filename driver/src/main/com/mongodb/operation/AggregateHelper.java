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

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.mongodb.MongoNamespace;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class AggregateHelper {

    static BsonDocument asCommandDocument(final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final AggregationOptions options) {
        BsonDocument commandDocument = new BsonDocument("aggregate", new BsonString(namespace.getCollectionName()));
        commandDocument.put("pipeline", asBsonArray(pipeline));
        if (options.getMaxTime(MILLISECONDS) > 0) {
            commandDocument.put("maxTimeMS", new BsonInt64(options.getMaxTime(MILLISECONDS)));
        }
        if (options.getOutputMode() == AggregationOptions.OutputMode.CURSOR) {
            BsonDocument cursor = new BsonDocument();
            if (options.getBatchSize() != null) {
                cursor.put("batchSize", new BsonInt32(options.getBatchSize()));
            }
            commandDocument.put("cursor", cursor);
        }
        if (options.getAllowDiskUse() != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(options.getAllowDiskUse()));
        }
        return commandDocument;
    }

    private static BsonArray asBsonArray(final List<BsonDocument> pipeline) {
        BsonArray bsonArray = new BsonArray();
        for (BsonDocument cur : pipeline) {
            bsonArray.add(cur);
        }
        return bsonArray;
    }

    private AggregateHelper() {
    }
}
