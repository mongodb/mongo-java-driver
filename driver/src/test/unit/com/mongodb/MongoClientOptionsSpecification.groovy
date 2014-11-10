/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

import javax.net.SocketFactory
import javax.net.ssl.SSLSocketFactory

import static java.util.concurrent.TimeUnit.MILLISECONDS

class MongoClientOptionsSpecification extends Specification {

    def 'should set the correct default values'() {
        given:
        def options = new MongoClientOptions.Builder().build()

        expect:
        options.getDescription() == null
        options.getWriteConcern() == WriteConcern.ACKNOWLEDGED
        options.getMinConnectionsPerHost() == 0
        options.getConnectionsPerHost() == 100
        options.getConnectTimeout() == 10000
        options.getReadPreference() == ReadPreference.primary()
        options.getThreadsAllowedToBlockForConnectionMultiplier() == 5
        !options.isSocketKeepAlive()
        !options.isSslEnabled()
        options.getSocketFactory() != null
        !(options.getSocketFactory() instanceof SSLSocketFactory)
        options.getDbDecoderFactory() == DefaultDBDecoder.FACTORY
        options.getDbEncoderFactory() == DefaultDBEncoder.FACTORY
        options.getAcceptableLatencyDifference() == 15
        options.isCursorFinalizerEnabled()
        options.getMinHeartbeatFrequency() == 10
    }

    @SuppressWarnings('UnnecessaryObjectReferences')
    def 'should handle illegal arguments'() {
        given:
        def builder = new MongoClientOptions.Builder()

        when:
        builder.heartbeatFrequency(0)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.minHeartbeatFrequency(0)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.writeConcern(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.readPreference(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.connectionsPerHost(0)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.minConnectionsPerHost(-1)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.connectTimeout(-1)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.threadsAllowedToBlockForConnectionMultiplier(0)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.dbDecoderFactory(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.dbEncoderFactory(null)
        then:
        thrown(IllegalArgumentException)

    }

    def 'should build with set options'() {
        given:
        def encoderFactory = new MyDBEncoderFactory()
        def options = MongoClientOptions.builder()
            .description('test')
            .readPreference(ReadPreference.secondary())
            .writeConcern(WriteConcern.JOURNAL_SAFE)
            .minConnectionsPerHost(30)
            .connectionsPerHost(500)
            .connectTimeout(100)
            .maxWaitTime(200)
            .maxConnectionIdleTime(300)
            .maxConnectionLifeTime(400)
            .threadsAllowedToBlockForConnectionMultiplier(1)
            .socketKeepAlive(true)
            .sslEnabled(true)
            .dbDecoderFactory(LazyDBDecoder.FACTORY)
            .heartbeatFrequency(5)
            .minHeartbeatFrequency(11)
            .heartbeatConnectTimeout(15)
            .heartbeatSocketTimeout(20)
            .acceptableLatencyDifference(25)
            .requiredReplicaSetName('test')
            .cursorFinalizerEnabled(false)
            .dbEncoderFactory(encoderFactory)
            .build()

        expect:
        options.getDescription() == 'test'
        options.getReadPreference() == ReadPreference.secondary()
        options.getWriteConcern() == WriteConcern.JOURNAL_SAFE
        options.getMaxWaitTime() == 200
        options.getMaxConnectionIdleTime() == 300
        options.getMaxConnectionLifeTime() == 400
        options.getMinConnectionsPerHost() == 30
        options.getConnectionsPerHost() == 500
        options.getConnectTimeout() == 100
        options.getThreadsAllowedToBlockForConnectionMultiplier() == 1
        options.isSocketKeepAlive()
        options.isSslEnabled()
        options.getDbDecoderFactory() == LazyDBDecoder.FACTORY
        options.getDbEncoderFactory() == encoderFactory
        options.getHeartbeatFrequency() == 5
        options.getMinHeartbeatFrequency() == 11
        options.getHeartbeatConnectTimeout() == 15
        options.getHeartbeatSocketTimeout() == 20
        options.getAcceptableLatencyDifference() == 25
        options.getRequiredReplicaSetName() == 'test'
        !options.isCursorFinalizerEnabled()
        options.getServerSettings().getHeartbeatFrequency(MILLISECONDS) == 5
        options.getServerSettings().getMinHeartbeatFrequency(MILLISECONDS) == 11
    }

    def 'should sslEnabled based on socketFactory'() {
        given:
        MongoClientOptions.Builder builder = MongoClientOptions.builder()
        SocketFactory socketFactory = SSLSocketFactory.getDefault()

        when:
        builder.socketFactory(socketFactory)
        then:
        builder.build().getSocketFactory() == socketFactory
        builder.sslEnabled(false)
        builder.build().getSocketFactory() != null
        !(builder.build().getSocketFactory() instanceof SSLSocketFactory)

        when:
        builder.sslEnabled(true)
        then:
        builder.build().getSocketFactory() instanceof SSLSocketFactory
    }

    private static class MyDBEncoderFactory implements DBEncoderFactory {
        @Override
        DBEncoder create() {
            new DefaultDBEncoder()
        }
    }
}
