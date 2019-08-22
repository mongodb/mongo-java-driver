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


package com.mongodb.reactivestreams.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.AbstractChangeStreamsTest;
import com.mongodb.client.MongoClient;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonDocument;

public class ChangeStreamsTest extends AbstractChangeStreamsTest {
    public ChangeStreamsTest(final String filename, final String description, final MongoNamespace namespace,
                             final MongoNamespace namespace2, final BsonDocument definition, final boolean skipTest) {
        super(filename, description, namespace, namespace2, definition, skipTest);
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return new SyncMongoClient(MongoClients.create(settings));
    }
}
