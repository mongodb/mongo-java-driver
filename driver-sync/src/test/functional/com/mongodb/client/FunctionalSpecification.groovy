/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client

import org.bson.Document
import spock.lang.Specification

class FunctionalSpecification extends Specification {
    protected MongoDatabase database;
    protected MongoCollection<Document> collection;

    def setup() {
        database = Fixture.getMongoClient().getDatabase(Fixture.getDefaultDatabaseName())
        collection = database.getCollection(getClass().getName())
        collection.drop();
    }

    def cleanup() {
        if (collection != null) {
            collection.drop()
        }
    }

    String getDatabaseName() {
        Fixture.getDefaultDatabaseName()
    }

    String getCollectionName() {
        collection.namespace.collectionName
    }
}
