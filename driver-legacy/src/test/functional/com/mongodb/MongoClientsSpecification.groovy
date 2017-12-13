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

import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ServerDescription
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandListener
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import org.bson.Document
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isStandalone
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.Fixture.getDefaultDatabaseName
import static com.mongodb.Fixture.getMongoClientURI

class  MongoClientsSpecification extends FunctionalSpecification {
    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isStandalone() })
    def 'application name should appear in the system.profile collection'() {
        given:
        def appName = 'appName1'
        def driverInfo = MongoDriverInformation.builder().driverName('myDriver').driverVersion('42').build()
        def client = new MongoClient(getMongoClientURI(MongoClientOptions.builder().applicationName(appName)), driverInfo)
        def database = client.getDatabase(getDatabaseName())
        def collection = database.getCollection(getCollectionName())

        def profileCollection = database.getCollection('system.profile')
        profileCollection.drop()

        database.runCommand(new Document('profile', 2))

        when:
        collection.count()

        then:
        Document profileDocument = profileCollection.find().first()
        profileDocument.get('appName') == appName

        cleanup:
        database?.runCommand(new Document('profile', 0))
        profileCollection?.drop()
        client?.close()
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should use server selector from MongoClientOptions'() {
        given:
        def expectedWinningAddresses = [] as Set
        def actualWinningAddresses = [] as Set
        def optionsBuilder = MongoClientOptions.builder()
        // select the suitable server with the highest port number
                .serverSelector { ClusterDescription clusterDescription ->
            def highestPortServer
            for (ServerDescription cur : clusterDescription.getServerDescriptions()) {
                if (highestPortServer == null || cur.address.port > highestPortServer.address.port) {
                    highestPortServer = cur
                }
            }
            if (highestPortServer == null) {
                return []
            }
            expectedWinningAddresses.add(highestPortServer.address)
            [highestPortServer]
        }.addCommandListener(new CommandListener() {
            // record each address actually used
            @Override
            void commandStarted(final CommandStartedEvent event) {
                actualWinningAddresses.add(event.connectionDescription.connectionId.serverId.address)
            }

            @Override
            void commandSucceeded(final CommandSucceededEvent event) {
            }

            @Override
            void commandFailed(final CommandFailedEvent event) {
            }
        })

        def client = new MongoClient(getMongoClientURI(optionsBuilder))
        def collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getCollectionName())
                .withReadPreference(ReadPreference.nearest())

        when:
        for (int i = 0; i < 10; i++) {
            collection.count()
        }

        then:
        expectedWinningAddresses.containsAll(actualWinningAddresses)
    }
}
