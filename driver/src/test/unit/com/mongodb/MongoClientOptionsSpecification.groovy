/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.event.ClusterListener
import com.mongodb.event.CommandListener
import com.mongodb.event.ServerListener
import com.mongodb.event.ServerMonitorListener
import spock.lang.IgnoreIf
import spock.lang.Specification

import javax.net.SocketFactory
import javax.net.ssl.SSLSocketFactory

import static com.mongodb.CustomMatchers.isTheSameAs
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS
import static spock.util.matcher.HamcrestSupport.expect

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
        !options.isSslInvalidHostNameAllowed()
        options.getSocketFactory() != null
        !(options.getSocketFactory() instanceof SSLSocketFactory)
        options.getDbDecoderFactory() == DefaultDBDecoder.FACTORY
        options.getDbEncoderFactory() == DefaultDBEncoder.FACTORY
        options.getLocalThreshold() == 15
        options.isCursorFinalizerEnabled()
        options.getHeartbeatFrequency() == 10000
        options.getMinHeartbeatFrequency() == 500
        options.getServerSelectionTimeout() == 30000

        options.getCommandListeners() == []
        options.getClusterListeners() == []
        options.getServerListeners() == []
        options.getServerMonitorListeners() == []

        options.connectionPoolSettings == ConnectionPoolSettings.builder().build()
        options.socketSettings == SocketSettings.builder().build()
        options.heartbeatSocketSettings == SocketSettings.builder().connectTimeout(20, SECONDS).readTimeout(20, SECONDS).build()
        options.serverSettings == ServerSettings.builder().heartbeatFrequency(10000, MILLISECONDS)
                                                .minHeartbeatFrequency(500, MILLISECONDS)
                                                .build()
        options.sslSettings == SslSettings.builder().build();
    }

    @SuppressWarnings('UnnecessaryObjectReferences')
    def 'should handle illegal arguments'() {
        given:
        def builder = new MongoClientOptions.Builder()

        when:
        builder.localThreshold(-1)
        then:
        thrown(IllegalArgumentException)

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
                                        .writeConcern(WriteConcern.JOURNALED)
                                        .minConnectionsPerHost(30)
                                        .connectionsPerHost(500)
                                        .connectTimeout(100)
                                        .socketTimeout(700)
                                        .serverSelectionTimeout(150)
                                        .maxWaitTime(200)
                                        .maxConnectionIdleTime(300)
                                        .maxConnectionLifeTime(400)
                                        .threadsAllowedToBlockForConnectionMultiplier(2)
                                        .socketKeepAlive(true)
                                        .sslEnabled(true)
                                        .sslInvalidHostNameAllowed(true)
                                        .dbDecoderFactory(LazyDBDecoder.FACTORY)
                                        .heartbeatFrequency(5)
                                        .minHeartbeatFrequency(11)
                                        .heartbeatConnectTimeout(15)
                                        .heartbeatSocketTimeout(20)
                                        .localThreshold(25)
                                        .requiredReplicaSetName('test')
                                        .cursorFinalizerEnabled(false)
                                        .dbEncoderFactory(encoderFactory)
                                        .build()

        expect:
        options.getDescription() == 'test'
        options.getReadPreference() == ReadPreference.secondary()
        options.getWriteConcern() == WriteConcern.JOURNALED
        options.getServerSelectionTimeout() == 150
        options.getMaxWaitTime() == 200
        options.getMaxConnectionIdleTime() == 300
        options.getMaxConnectionLifeTime() == 400
        options.getMinConnectionsPerHost() == 30
        options.getConnectionsPerHost() == 500
        options.getConnectTimeout() == 100
        options.getSocketTimeout() == 700
        options.getThreadsAllowedToBlockForConnectionMultiplier() == 2
        options.isSocketKeepAlive()
        options.isSslEnabled()
        options.isSslInvalidHostNameAllowed()
        options.getDbDecoderFactory() == LazyDBDecoder.FACTORY
        options.getDbEncoderFactory() == encoderFactory
        options.getHeartbeatFrequency() == 5
        options.getMinHeartbeatFrequency() == 11
        options.getHeartbeatConnectTimeout() == 15
        options.getHeartbeatSocketTimeout() == 20
        options.getLocalThreshold() == 25
        options.getRequiredReplicaSetName() == 'test'
        !options.isCursorFinalizerEnabled()
        options.getServerSettings().getHeartbeatFrequency(MILLISECONDS) == 5
        options.getServerSettings().getMinHeartbeatFrequency(MILLISECONDS) == 11

        options.connectionPoolSettings == ConnectionPoolSettings.builder().maxSize(500).minSize(30).maxWaitQueueSize(1000)
                                                                .maxWaitTime(200, MILLISECONDS).maxConnectionLifeTime(400, MILLISECONDS)
                                                                .maxConnectionIdleTime(300, MILLISECONDS).build()
        options.socketSettings == SocketSettings.builder().connectTimeout(100, MILLISECONDS).readTimeout(700, MILLISECONDS)
                                                .keepAlive(true).build()
        options.heartbeatSocketSettings == SocketSettings.builder().connectTimeout(15, MILLISECONDS).readTimeout(20, MILLISECONDS)
                                                         .keepAlive(true).build()
        options.serverSettings == ServerSettings.builder().minHeartbeatFrequency(11, MILLISECONDS).heartbeatFrequency(5, MILLISECONDS)
                                                .build()
        options.sslSettings == SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build()
    }

    @IgnoreIf({ System.getProperty('java.version').startsWith('1.6.') })
    def 'should set sslEnabled based on socketFactory'() {
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

    def 'should be easy to create new options from existing'() {
        when:
        def options = MongoClientOptions.builder()
                .description('test')
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .minConnectionsPerHost(30)
                .connectionsPerHost(500)
                .connectTimeout(100)
                .socketTimeout(700)
                .serverSelectionTimeout(150)
                .maxWaitTime(200)
                .maxConnectionIdleTime(300)
                .maxConnectionLifeTime(400)
                .threadsAllowedToBlockForConnectionMultiplier(2)
                .socketKeepAlive(true)
                .sslEnabled(true)
                .sslInvalidHostNameAllowed(true)
                .dbDecoderFactory(LazyDBDecoder.FACTORY)
                .heartbeatFrequency(5)
                .minHeartbeatFrequency(11)
                .heartbeatConnectTimeout(15)
                .heartbeatSocketTimeout(20)
                .localThreshold(25)
                .requiredReplicaSetName('test')
                .cursorFinalizerEnabled(false)
                .dbEncoderFactory(new MyDBEncoderFactory())
                .addCommandListener(Mock(CommandListener))
                .addClusterListener(Mock(ClusterListener))
                .addServerListener(Mock(ServerListener))
                .addServerMonitorListener(Mock(ServerMonitorListener))
                .build()

        then:
        expect options, isTheSameAs(MongoClientOptions.builder(options).build())
    }

    def 'should throw MongoInternalException mentioning MongoClientOptions if sslEnabled is true and sslInvalidHostNameAllowed is false'() {
        given:
        String javaVersion = System.getProperty('java.version')
        when:

        System.setProperty('java.version', '1.6.0_45')
        MongoClientOptions.builder().sslEnabled(true).build()

        then:
        def e = thrown(MongoInternalException)
        e.message.contains('MongoClientOptions.sslInvalidHostNameAllowed')

        cleanup:
        System.setProperty('java.version', javaVersion)
    }

    def 'should add command listeners'() {
        given:
        CommandListener commandListenerOne = Mock(CommandListener)
        CommandListener commandListenerTwo = Mock(CommandListener)
        CommandListener commandListenerThree = Mock(CommandListener)

        when:
        def options = MongoClientOptions.builder()
                                        .build()

        then:
        options.commandListeners.size() == 0

        when:
        options = MongoClientOptions.builder()
                                    .addCommandListener(commandListenerOne)
                                    .build()

        then:
        options.commandListeners.size() == 1
        options.commandListeners[0].is commandListenerOne

        when:
        options = MongoClientOptions.builder()
                                    .addCommandListener(commandListenerOne)
                                    .addCommandListener(commandListenerTwo)
                                    .build()

        then:
        options.commandListeners.size() == 2
        options.commandListeners[0].is commandListenerOne
        options.commandListeners[1].is commandListenerTwo

        when:
        def copiedOptions = MongoClientOptions.builder(options).addCommandListener(commandListenerThree).build()

        then:
        copiedOptions.commandListeners.size() == 3
        copiedOptions.commandListeners[0].is commandListenerOne
        copiedOptions.commandListeners[1].is commandListenerTwo
        copiedOptions.commandListeners[2].is commandListenerThree
        options.commandListeners.size() == 2
        options.commandListeners[0].is commandListenerOne
        options.commandListeners[1].is commandListenerTwo
    }

    def 'should add cluster listeners'() {
        given:
        ClusterListener clusterListenerOne = Mock(ClusterListener)
        ClusterListener clusterListenerTwo = Mock(ClusterListener)
        ClusterListener clusterListenerThree = Mock(ClusterListener)

        when:
        def options = MongoClientOptions.builder()
                .build()

        then:
        options.clusterListeners.size() == 0

        when:
        options = MongoClientOptions.builder()
                .addClusterListener(clusterListenerOne)
                .build()

        then:
        options.clusterListeners.size() == 1
        options.clusterListeners[0].is clusterListenerOne

        when:
        options = MongoClientOptions.builder()
                .addClusterListener(clusterListenerOne)
                .addClusterListener(clusterListenerTwo)
                .build()

        then:
        options.clusterListeners.size() == 2
        options.clusterListeners[0].is clusterListenerOne
        options.clusterListeners[1].is clusterListenerTwo

        when:
        def copiedOptions = MongoClientOptions.builder(options).addClusterListener(clusterListenerThree).build()

        then:
        copiedOptions.clusterListeners.size() == 3
        copiedOptions.clusterListeners[0].is clusterListenerOne
        copiedOptions.clusterListeners[1].is clusterListenerTwo
        copiedOptions.clusterListeners[2].is clusterListenerThree
        options.clusterListeners.size() == 2
        options.clusterListeners[0].is clusterListenerOne
        options.clusterListeners[1].is clusterListenerTwo
    }

    def 'should add server listeners'() {
        given:
        ServerListener serverListenerOne = Mock(ServerListener)
        ServerListener serverListenerTwo = Mock(ServerListener)
        ServerListener serverListenerThree = Mock(ServerListener)

        when:
        def options = MongoClientOptions.builder()
                .build()

        then:
        options.serverListeners.size() == 0

        when:
        options = MongoClientOptions.builder()
                .addServerListener(serverListenerOne)
                .build()

        then:
        options.serverListeners.size() == 1
        options.serverListeners[0].is serverListenerOne

        when:
        options = MongoClientOptions.builder()
                .addServerListener(serverListenerOne)
                .addServerListener(serverListenerTwo)
                .build()

        then:
        options.serverListeners.size() == 2
        options.serverListeners[0].is serverListenerOne
        options.serverListeners[1].is serverListenerTwo

        when:
        def copiedOptions = MongoClientOptions.builder(options).addServerListener(serverListenerThree).build()

        then:
        copiedOptions.serverListeners.size() == 3
        copiedOptions.serverListeners[0].is serverListenerOne
        copiedOptions.serverListeners[1].is serverListenerTwo
        copiedOptions.serverListeners[2].is serverListenerThree
        options.serverListeners.size() == 2
        options.serverListeners[0].is serverListenerOne
        options.serverListeners[1].is serverListenerTwo
    }

    def 'should add server monitor listeners'() {
        given:
        ServerMonitorListener serverMonitorListenerOne = Mock(ServerMonitorListener)
        ServerMonitorListener serverMonitorListenerTwo = Mock(ServerMonitorListener)
        ServerMonitorListener serverMonitorListenerThree = Mock(ServerMonitorListener)

        when:
        def options = MongoClientOptions.builder()
                .build()

        then:
        options.serverMonitorListeners.size() == 0

        when:
        options = MongoClientOptions.builder()
                .addServerMonitorListener(serverMonitorListenerOne)
                .build()

        then:
        options.serverMonitorListeners.size() == 1
        options.serverMonitorListeners[0].is serverMonitorListenerOne

        when:
        options = MongoClientOptions.builder()
                .addServerMonitorListener(serverMonitorListenerOne)
                .addServerMonitorListener(serverMonitorListenerTwo)
                .build()

        then:
        options.serverMonitorListeners.size() == 2
        options.serverMonitorListeners[0].is serverMonitorListenerOne
        options.serverMonitorListeners[1].is serverMonitorListenerTwo

        when:
        def copiedOptions = MongoClientOptions.builder(options).addServerMonitorListener(serverMonitorListenerThree).build()

        then:
        copiedOptions.serverMonitorListeners.size() == 3
        copiedOptions.serverMonitorListeners[0].is serverMonitorListenerOne
        copiedOptions.serverMonitorListeners[1].is serverMonitorListenerTwo
        copiedOptions.serverMonitorListeners[2].is serverMonitorListenerThree
        options.serverMonitorListeners.size() == 2
        options.serverMonitorListeners[0].is serverMonitorListenerOne
        options.serverMonitorListeners[1].is serverMonitorListenerTwo
    }

    def 'builder should copy all values from the existing MongoClientOptions'() {
        given:
        def options = Mock(MongoClientOptions)


        when:
        MongoClientOptions.builder(options)

        then:
        1 * options.isAlwaysUseMBeans()
        1 * options.getCodecRegistry()
        1 * options.getConnectionsPerHost()
        1 * options.getConnectTimeout()
        1 * options.isCursorFinalizerEnabled()
        1 * options.getDbDecoderFactory()
        1 * options.getDbEncoderFactory()
        1 * options.getDescription()
        1 * options.getHeartbeatConnectTimeout()
        1 * options.getHeartbeatFrequency()
        1 * options.getHeartbeatSocketTimeout()
        1 * options.getLocalThreshold()
        1 * options.getMaxConnectionIdleTime()
        1 * options.getMaxConnectionLifeTime()
        1 * options.getMaxWaitTime()
        1 * options.getMinConnectionsPerHost()
        1 * options.getMinHeartbeatFrequency()
        1 * options.getReadPreference()
        1 * options.getRequiredReplicaSetName()
        1 * options.getServerSelectionTimeout()
        1 * options.getSocketFactory()
        1 * options.isSocketKeepAlive()
        1 * options.getSocketTimeout()
        1 * options.isSslEnabled()
        1 * options.isSslInvalidHostNameAllowed()
        1 * options.getThreadsAllowedToBlockForConnectionMultiplier()
        1 * options.getWriteConcern()
        1 * options.getReadConcern()
        1 * options.getCommandListeners() >> Collections.unmodifiableList(Collections.emptyList())
        1 * options.getClusterListeners() >> Collections.unmodifiableList(Collections.emptyList())
        1 * options.getServerListeners() >> Collections.unmodifiableList(Collections.emptyList())
        1 * options.getServerMonitorListeners() >> Collections.unmodifiableList(Collections.emptyList())

        0 * options._ // Ensure no other interactions
    }

    def 'should only have the following fields in the builder'() {
        when:
        // A regression test so that if any more methods are added then the builder(final MongoClientOptions options) should be updated
        def actual = MongoClientOptions.Builder.declaredFields.grep {  !it.synthetic } *.name.sort()
        def expected = ['alwaysUseMBeans', 'clusterListeners', 'codecRegistry', 'commandListeners', 'connectTimeout',
                        'cursorFinalizerEnabled', 'dbDecoderFactory', 'dbEncoderFactory', 'description', 'heartbeatConnectTimeout',
                        'heartbeatFrequency', 'heartbeatSocketTimeout', 'localThreshold', 'maxConnectionIdleTime', 'maxConnectionLifeTime',
                        'maxConnectionsPerHost', 'maxWaitTime', 'minConnectionsPerHost', 'minHeartbeatFrequency', 'readConcern',
                        'readPreference', 'requiredReplicaSetName', 'serverListeners', 'serverMonitorListeners', 'serverSelectionTimeout',
                        'socketFactory', 'socketKeepAlive', 'socketTimeout', 'sslEnabled', 'sslInvalidHostNameAllowed',
                        'threadsAllowedToBlockForConnectionMultiplier', 'writeConcern']

        then:
        actual == expected
    }

    private static class MyDBEncoderFactory implements DBEncoderFactory {
        @Override
        DBEncoder create() {
            new DefaultDBEncoder()
        }
    }
}
