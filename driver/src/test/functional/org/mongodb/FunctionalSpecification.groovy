/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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





package org.mongodb

import spock.lang.Specification

import static org.mongodb.Fixture.getMongoClient
import static org.mongodb.Fixture.initialiseCollection

class FunctionalSpecification extends Specification {
    protected static MongoDatabase database;
    protected MongoCollection<Document> collection;

    def setupSpec() {
        if (database == null) {
            database = getMongoClient().getDatabase('DriverTest-' + System.nanoTime());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
    }

    def setup() {
        collection = initialiseCollection(database, getClass().getName());
    }

    def cleanup() {
        if (collection != null) {
            collection.tools().drop();
        }
    }

    String getDatabaseName() {
        database.getName();
    }

    String getCollectionName() {
        collection.getName();
    }

    MongoNamespace getNamespace() {
        new MongoNamespace(getDatabaseName(), getCollectionName())
    }

    static class ShutdownHook extends Thread {
        @Override
        void run() {
            if (database != null) {
                database.tools().drop();
                getMongoClient().close();
            }
        }
    }

}
