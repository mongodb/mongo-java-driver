/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb

import com.mongodb.client.MongoDriverInformation
import org.bson.Document
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.isStandalone
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.Fixture.getMongoClientURI

class MongoClientsSpecification extends FunctionalSpecification {
    @IgnoreIf({ !serverVersionAtLeast([3, 3, 9]) || !isStandalone() })
    def 'application name should appear in the system.profile collection'() {
        given:
        def appName = 'appName1'
        def driverInfo = MongoDriverInformation.builder().driverName('myDriver').driverVersion('42').build()
        def client = new MongoClient(getMongoClientURI(MongoClientOptions.builder().applicationName(appName)), driverInfo)
        def database = client.getDatabase(getDatabaseName())
        def collection = database.getCollection(getCollectionName())
        database.runCommand(new Document('profile', 2))

        when:
        collection.count()

        then:
        Document profileDocument = database.getCollection('system.profile').find().first()
        profileDocument.get('appName') == appName

        cleanup:
        if (database != null) {
            database.runCommand(new Document('profile', 0))
        }
        client?.close()
    }

}
