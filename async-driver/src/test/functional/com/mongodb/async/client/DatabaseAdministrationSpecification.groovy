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

package com.mongodb.async.client

class DatabaseAdministrationSpecification extends FunctionalSpecification {

    def 'Drop should drop the database'() {
        when:
        def client = Fixture.getMongoClient()
        def database = client.getDatabase(databaseName)
        database.tools().createCollection(collectionName).get()

        then:
        client.tools().getDatabaseNames().get().contains(databaseName)

        when:
        database.tools().drop().get()

        then:
        !client.tools().getDatabaseNames().get().contains(databaseName)
    }

    def 'rename collection should rename the collection name'() {
        when:
        def client = Fixture.getMongoClient()
        def database = client.getDatabase(databaseName)
        database.tools().createCollection(collectionName).get()

        then:
        database.tools().getCollectionNames().get().contains(collectionName)

        when:
        database.tools().renameCollection(collectionName, 'NewCollection1234').get()

        then:
        !database.tools().getCollectionNames().get().contains(collectionName)
        database.tools().getCollectionNames().get().contains('NewCollection1234')
    }

}
