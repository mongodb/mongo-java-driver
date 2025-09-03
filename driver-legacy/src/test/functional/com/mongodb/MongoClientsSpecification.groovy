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

package com.mongodb

import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ServerDescription
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandListener
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import spock.lang.IgnoreIf

import static Fixture.getDefaultDatabaseName
import static Fixture.getMongoClientURI
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.Fixture.getOptions

class  MongoClientsSpecification extends FunctionalSpecification {

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should use server selector from MongoClientOptions'() {
        given:
        def expectedWinningAddresses = [] as Set
        def actualWinningAddresses = [] as Set
        def optionsBuilder = MongoClientOptions.builder(getOptions())
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
            collection.countDocuments()
        }

        then:
        expectedWinningAddresses.containsAll(actualWinningAddresses)
    }
}
