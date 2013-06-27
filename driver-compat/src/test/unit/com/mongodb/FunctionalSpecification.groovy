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

package com.mongodb

import spock.lang.Specification

import static com.mongodb.Fixture.getMongoClient

class FunctionalSpecification extends Specification {
    protected static DB database;
    protected DBCollection collection;

    def setupSpec() {
        if (database == null) {
            database = getMongoClient().getDB('DriverTest-' + System.nanoTime());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
    }

    def setup() {
        collection = database.getCollection(getClass().getName());
        collection.drop();
    }

    def cleanup() {
        if (collection != null) {
            collection.drop();
        }
    }

    String getDatabaseName() {
        database.getName();
    }

    String getCollectionName() {
        collection.getName();
    }

    static class ShutdownHook extends Thread {
        @Override
        void run() {
            if (database != null) {
                database.dropDatabase();
                getMongoClient().close();
            }
        }
    }
}
