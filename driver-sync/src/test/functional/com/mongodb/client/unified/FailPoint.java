/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.client.MongoClient;
import org.bson.BsonDocument;
import org.bson.BsonString;

final class FailPoint {
    private final MongoClient client;
    private final BsonDocument failPointDocument;

    protected FailPoint(final BsonDocument operation, final Entities entities) {
        BsonDocument arguments = operation.getDocument("arguments");
        client = entities.getClient(arguments.getString("client").getValue());
        failPointDocument = arguments.getDocument("failPoint");
    }

    public void executeFailPoint() {
        client.getDatabase("admin").runCommand(failPointDocument);
    }

    public void disableFailPoint() {
        client.getDatabase("admin")
                .runCommand(new BsonDocument("configureFailPoint",
                        failPointDocument.getString("configureFailPoint"))
                        .append("mode", new BsonString("off")));
    }
}
