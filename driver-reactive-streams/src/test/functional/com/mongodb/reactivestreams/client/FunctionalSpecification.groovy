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

package com.mongodb.reactivestreams.client

import com.mongodb.MongoNamespace
import org.bson.Document
import spock.lang.Specification

import static Fixture.dropDatabase
import static Fixture.initializeCollection
import static com.mongodb.ClusterFixture.getDefaultDatabaseName
import static com.mongodb.reactivestreams.client.Fixture.drop
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabase
import static com.mongodb.reactivestreams.client.Fixture.waitForLastServerSessionPoolRelease

class FunctionalSpecification extends Specification {
    protected MongoDatabase database
    protected MongoCollection<Document> collection

    def setupSpec() {
        dropDatabase(getDefaultDatabaseName())
    }

    def cleanupSpec() {
        dropDatabase(getDefaultDatabaseName())
    }

    def setup() {
        database = getDefaultDatabase()
        collection = initializeCollection(new MongoNamespace(database.getName(), getClass().getName()))
        drop(collection.getNamespace())
    }

    def cleanup() {
        if (collection != null) {
            drop(collection.getNamespace())
        }

        waitForLastServerSessionPoolRelease()
    }

    String getDatabaseName() {
        database.getName()
    }

    String getCollectionName() {
        collection.getNamespace().getCollectionName()
    }

    MongoNamespace getNamespace() {
        new MongoNamespace(getDatabaseName(), getCollectionName())
    }
}
