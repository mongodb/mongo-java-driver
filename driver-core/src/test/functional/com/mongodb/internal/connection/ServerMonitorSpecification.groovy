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

package com.mongodb.internal.connection

import com.mongodb.MongoSocketException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.Tag
import com.mongodb.TagSet
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.ServerType
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import com.mongodb.internal.inject.SameObjectProvider
import org.bson.types.ObjectId

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.getCredentialWithCache
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerConnectionState.CONNECTING
import static com.mongodb.connection.ServerDescription.builder
import static com.mongodb.internal.connection.DefaultServerMonitor.shouldLogStageChange
import static java.util.Arrays.asList

class ServerMonitorSpecification extends OperationFunctionalSpecification {
    ServerDescription newDescription
    ServerMonitor serverMonitor
    CountDownLatch latch = new CountDownLatch(1)

    def cleanup() {
        serverMonitor?.close()
    }

    def 'should have positive round trip time'() {
        given:
        initializeServerMonitor(getPrimary())

        when:
        latch.await()

        then:
        newDescription.roundTripTimeNanos > 0
    }

    def 'should report current exception'() {
        given:
        initializeServerMonitor(new ServerAddress('some_unknown_server_name:34567'))

        when:
        latch.await()

        then:
        newDescription.exception instanceof MongoSocketException
    }

    def 'should log state change if significant properties have changed'() {
        given:
        ServerDescription.Builder builder = createBuilder();
        ServerDescription description = builder.build();
        ServerDescription otherDescription

        expect:
        !shouldLogStageChange(description, builder.build())

        when:
        otherDescription = createBuilder().address(new ServerAddress('localhost:27018')).build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().type(ServerType.STANDALONE).build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().tagSet(null).build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().setName('test2').build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().primary('localhost:27018').build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().canonicalAddress('localhost:27018').build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().hosts(new HashSet<String>(asList('localhost:27018'))).build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().arbiters(new HashSet<String>(asList('localhost:27018'))).build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().passives(new HashSet<String>(asList('localhost:27018'))).build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().ok(false).build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().state(CONNECTING).build();

        then:
        shouldLogStageChange(description, otherDescription)

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().electionId(new ObjectId()).build();

        then:
        shouldLogStageChange(description, otherDescription)

        when:
        otherDescription = createBuilder().setVersion(3).build();

        then:
        shouldLogStageChange(description, otherDescription)

        // test exception state changes
        shouldLogStageChange(createBuilder().exception(new IOException()).build(),
                createBuilder().exception(new RuntimeException()).build())
        shouldLogStageChange(createBuilder().exception(new IOException('message one')).build(),
                createBuilder().exception(new IOException('message two')).build())
    }

    private static ServerDescription.Builder createBuilder() {
        builder().ok(true)
                .state(CONNECTED)
                .address(new ServerAddress())
                .type(ServerType.SHARD_ROUTER)
                .tagSet(new TagSet(asList(new Tag('dc', 'ny'))))
                .setName('test')
                .primary('localhost:27017')
                .canonicalAddress('localhost:27017')
                .hosts(new HashSet<String>(asList('localhost:27017', 'localhost:27018')))
                .passives(new HashSet<String>(asList('localhost:27019')))
                .arbiters(new HashSet<String>(asList('localhost:27020')))
                .electionId(new ObjectId('abcdabcdabcdabcdabcdabcd'))
                .setVersion(2)
    }

    def initializeServerMonitor(ServerAddress address) {
        SdamServerDescriptionManager sdam = new SdamServerDescriptionManager() {
            @Override
            void update(final ServerDescription candidateDescription) {
                assert candidateDescription != null
                newDescription = candidateDescription
                latch.countDown()
            }

            @Override
            void handleExceptionBeforeHandshake(final SdamServerDescriptionManager.SdamIssue sdamIssue) {
                throw new UnsupportedOperationException()
            }

            @Override
            void handleExceptionAfterHandshake(final SdamServerDescriptionManager.SdamIssue sdamIssue) {
                throw new UnsupportedOperationException()
            }

            @Override
            SdamServerDescriptionManager.SdamIssue.Context context() {
                throw new UnsupportedOperationException()
            }

            @Override
            SdamServerDescriptionManager.SdamIssue.Context context(final InternalConnection connection) {
                throw new UnsupportedOperationException()
            }
        }
        serverMonitor = new DefaultServerMonitor(new ServerId(new ClusterId(), address), ServerSettings.builder().build(),
                new ClusterClock(),
                new InternalStreamConnectionFactory(SINGLE, new SocketStreamFactory(SocketSettings.builder()
                        .connectTimeout(500, TimeUnit.MILLISECONDS)
                        .build(),
                        getSslSettings()), getCredentialWithCache(), null, null, [], null, getServerApi()),
                getServerApi(), SameObjectProvider.initialized(sdam))
        serverMonitor.start()
        serverMonitor
    }
}
