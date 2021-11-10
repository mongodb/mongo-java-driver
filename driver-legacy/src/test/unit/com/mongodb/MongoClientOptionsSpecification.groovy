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

import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.event.ClusterListener
import com.mongodb.event.CommandListener
import com.mongodb.event.ConnectionPoolListener
import com.mongodb.event.ServerListener
import com.mongodb.event.ServerMonitorListener
import com.mongodb.selector.ServerSelector
import org.bson.UuidRepresentation
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import javax.net.ssl.SSLContext

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientOptionsSpecification extends Specification {

    def 'should set the correct default values'() {
        given:
        def options = new MongoClientOptions.Builder().build()

        expect:
        options.getApplicationName() == null
        options.getWriteConcern() == WriteConcern.ACKNOWLEDGED
        options.getRetryWrites()
        options.getRetryReads()
        options.getCodecRegistry() == MongoClientSettings.defaultCodecRegistry
        options.getUuidRepresentation() == UuidRepresentation.UNSPECIFIED
        options.getMinConnectionsPerHost() == 0
        options.getConnectionsPerHost() == 100
        options.getMaxConnecting() == 2
        options.getConnectTimeout() == 10000
        options.getReadPreference() == ReadPreference.primary()
        options.getServerSelector() == null
        !options.isSslEnabled()
        !options.isSslInvalidHostNameAllowed()
        options.getSslContext() == null
        options.getDbDecoderFactory() == DefaultDBDecoder.FACTORY
        options.getDbEncoderFactory() == DefaultDBEncoder.FACTORY
        options.getLocalThreshold() == 15
        options.isCursorFinalizerEnabled()
        options.getHeartbeatFrequency() == 10000
        options.getMinHeartbeatFrequency() == 500
        options.getServerSelectionTimeout() == 30000

        options.getCommandListeners() == []
        options.getClusterListeners() == []
        options.getConnectionPoolListeners() == []
        options.getServerListeners() == []
        options.getServerMonitorListeners() == []

        options.connectionPoolSettings == ConnectionPoolSettings.builder().build()
        options.socketSettings == SocketSettings.builder().build()
        options.heartbeatSocketSettings == SocketSettings.builder().connectTimeout(20, SECONDS).readTimeout(20, SECONDS).build()
        options.serverSettings == ServerSettings.builder().heartbeatFrequency(10000, MILLISECONDS)
                                                .minHeartbeatFrequency(500, MILLISECONDS)
                                                .build()
        options.sslSettings == SslSettings.builder().build()
        options.compressorList == []
        options.getAutoEncryptionSettings() == null
        options.getServerApi() == null
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
        builder.connectionsPerHost(-1)
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
        builder.dbDecoderFactory(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.dbEncoderFactory(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.compressorList(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.uuidRepresentation(null)
        then:
        thrown(IllegalArgumentException)
    }

    def 'should build with set options'() {
        given:
        def encoderFactory = new MyDBEncoderFactory()
        def serverSelector = Mock(ServerSelector)
        def commandListener = Mock(CommandListener)
        def clusterListener = Mock(ClusterListener)
        def serverListener = Mock(ServerListener)
        def serverMonitorListener = Mock(ServerMonitorListener)
        def autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace('admin.keys')
                .kmsProviders(['local': ['key': new byte[64]]])
                .build()
        def codecRegistry = Mock(CodecRegistry)
        def serverApi = ServerApi.builder().version(ServerApiVersion.V1).build()

        when:
        def options = MongoClientOptions.builder()
                                        .applicationName('appName')
                                        .readPreference(ReadPreference.secondary())
                                        .retryWrites(true)
                                        .retryReads(false)
                                        .writeConcern(WriteConcern.JOURNALED)
                                        .readConcern(ReadConcern.MAJORITY)
                                        .minConnectionsPerHost(30)
                                        .connectionsPerHost(500)
                                        .connectTimeout(100)
                                        .socketTimeout(700)
                                        .serverSelector(serverSelector)
                                        .serverSelectionTimeout(150)
                                        .maxWaitTime(200)
                                        .maxConnectionIdleTime(300)
                                        .maxConnectionLifeTime(400)
                                        .maxConnecting(1)
                                        .sslEnabled(true)
                                        .sslInvalidHostNameAllowed(true)
                                        .sslContext(SSLContext.getDefault())
                                        .dbDecoderFactory(LazyDBDecoder.FACTORY)
                                        .heartbeatFrequency(5)
                                        .minHeartbeatFrequency(11)
                                        .heartbeatConnectTimeout(15)
                                        .heartbeatSocketTimeout(20)
                                        .localThreshold(25)
                                        .requiredReplicaSetName('test')
                                        .cursorFinalizerEnabled(false)
                                        .dbEncoderFactory(encoderFactory)
                                        .compressorList([MongoCompressor.createZlibCompressor()])
                                        .autoEncryptionSettings(autoEncryptionSettings)
                                        .codecRegistry(codecRegistry)
                                        .addCommandListener(commandListener)
                                        .addClusterListener(clusterListener)
                                        .addServerListener(serverListener)
                                        .addServerMonitorListener(serverMonitorListener)
                                        .uuidRepresentation(UuidRepresentation.C_SHARP_LEGACY)
                                        .serverApi(serverApi)
                                        .build()

        then:
        options.getApplicationName() == 'appName'
        options.getReadPreference() == ReadPreference.secondary()
        options.getWriteConcern() == WriteConcern.JOURNALED
        options.getReadConcern() == ReadConcern.MAJORITY
        options.getServerSelector() == serverSelector
        options.getRetryWrites()
        !options.getRetryReads()
        options.getServerSelectionTimeout() == 150
        options.getMaxWaitTime() == 200
        options.getMaxConnectionIdleTime() == 300
        options.getMaxConnectionLifeTime() == 400
        options.getMaxConnecting() == 1
        options.getMinConnectionsPerHost() == 30
        options.getConnectionsPerHost() == 500
        options.getConnectTimeout() == 100
        options.getSocketTimeout() == 700
        options.isSslEnabled()
        options.isSslInvalidHostNameAllowed()
        options.getSslContext() == SSLContext.getDefault()
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

        def connectionPoolSettings = ConnectionPoolSettings.builder().maxSize(500).minSize(30)
                .maxWaitTime(200, MILLISECONDS).maxConnectionLifeTime(400, MILLISECONDS)
                .maxConnectionIdleTime(300, MILLISECONDS)
                .maxConnecting(options.getMaxConnecting()).build()
        def socketSettings = SocketSettings.builder().connectTimeout(100, MILLISECONDS)
                .readTimeout(700, MILLISECONDS)
                .build()
        def heartbeatSocketSettings = SocketSettings.builder().connectTimeout(15, MILLISECONDS)
                .readTimeout(20, MILLISECONDS)
                .build()
        def serverSettings = ServerSettings.builder().minHeartbeatFrequency(11, MILLISECONDS)
                .heartbeatFrequency(5, MILLISECONDS)
                .addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener)
                .build()
        def sslSettings = SslSettings.builder().enabled(true).invalidHostNameAllowed(true)
                .context(SSLContext.getDefault()).build()

        options.connectionPoolSettings == connectionPoolSettings
        options.socketSettings == socketSettings
        options.heartbeatSocketSettings == heartbeatSocketSettings
        options.serverSettings == serverSettings
        options.sslSettings == sslSettings
        options.compressorList == [MongoCompressor.createZlibCompressor()]
        options.getAutoEncryptionSettings() == autoEncryptionSettings
        options.getClusterListeners() == [clusterListener]
        options.getCommandListeners() == [commandListener]
        options.getServerListeners() == [serverListener]
        options.getServerMonitorListeners() == [serverMonitorListener]
        options.getUuidRepresentation() == UuidRepresentation.C_SHARP_LEGACY
        options.getServerApi() == serverApi

        when:
        def credential = MongoCredential.createCredential('user1', 'app1', 'pwd'.toCharArray())
        def settings = options.asMongoClientSettings([new ServerAddress('host1')], null, SINGLE,
                credential)

        then:
        settings.credential == credential
        settings.readPreference == ReadPreference.secondary()
        settings.applicationName == 'appName'
        settings.writeConcern == WriteConcern.JOURNALED
        settings.retryWrites
        !settings.retryReads
        settings.autoEncryptionSettings == autoEncryptionSettings
        settings.codecRegistry == codecRegistry
        settings.commandListeners == [commandListener]
        settings.compressorList == [MongoCompressor.createZlibCompressor()]
        settings.readConcern == ReadConcern.MAJORITY
        settings.uuidRepresentation == UuidRepresentation.C_SHARP_LEGACY
        settings.serverApi == serverApi

        settings.clusterSettings == ClusterSettings.builder()
                .hosts([new ServerAddress('host1')])
                .mode(SINGLE)
                .requiredReplicaSetName('test')
                .serverSelector(serverSelector)
                .serverSelectionTimeout(150, MILLISECONDS)
                .localThreshold(25, MILLISECONDS)
                .addClusterListener(clusterListener)
                .build()
        settings.serverSettings == serverSettings
        settings.connectionPoolSettings == connectionPoolSettings
        settings.serverSettings == serverSettings
        settings.socketSettings == socketSettings
        settings.heartbeatSocketSettings == heartbeatSocketSettings

        when:
        def optionsFromSettings = MongoClientOptions.builder(settings).build()

        then:
        optionsFromSettings.getApplicationName() == 'appName'
        optionsFromSettings.getReadPreference() == ReadPreference.secondary()
        optionsFromSettings.getWriteConcern() == WriteConcern.JOURNALED
        optionsFromSettings.getReadConcern() == ReadConcern.MAJORITY
        optionsFromSettings.getServerSelector() == serverSelector
        optionsFromSettings.getRetryWrites()
        !optionsFromSettings.getRetryReads()
        optionsFromSettings.getServerSelectionTimeout() == 150
        optionsFromSettings.getMaxWaitTime() == 200
        optionsFromSettings.getMaxConnectionIdleTime() == 300
        optionsFromSettings.getMaxConnectionLifeTime() == 400
        optionsFromSettings.getMaxConnecting() == settings.connectionPoolSettings.maxConnecting
        optionsFromSettings.getMinConnectionsPerHost() == 30
        optionsFromSettings.getConnectionsPerHost() == 500
        optionsFromSettings.getConnectTimeout() == 100
        optionsFromSettings.getSocketTimeout() == 700
        optionsFromSettings.isSslEnabled()
        optionsFromSettings.isSslInvalidHostNameAllowed()
        optionsFromSettings.getSslContext() == SSLContext.getDefault()
        optionsFromSettings.getHeartbeatFrequency() == 5
        optionsFromSettings.getMinHeartbeatFrequency() == 11
        optionsFromSettings.getHeartbeatConnectTimeout() == 15
        optionsFromSettings.getHeartbeatSocketTimeout() == 20
        optionsFromSettings.getLocalThreshold() == 25
        optionsFromSettings.getRequiredReplicaSetName() == 'test'
        optionsFromSettings.getServerSettings().getHeartbeatFrequency(MILLISECONDS) == 5
        optionsFromSettings.getServerSettings().getMinHeartbeatFrequency(MILLISECONDS) == 11
        optionsFromSettings.connectionPoolSettings == connectionPoolSettings
        optionsFromSettings.socketSettings == socketSettings
        optionsFromSettings.heartbeatSocketSettings == heartbeatSocketSettings
        optionsFromSettings.serverSettings == serverSettings
        optionsFromSettings.sslSettings == sslSettings
        optionsFromSettings.compressorList == [MongoCompressor.createZlibCompressor()]
        optionsFromSettings.getAutoEncryptionSettings() == autoEncryptionSettings
        optionsFromSettings.getClusterListeners() == [clusterListener]
        optionsFromSettings.getCommandListeners() == [commandListener]
        optionsFromSettings.getServerListeners() == [serverListener]
        optionsFromSettings.getServerMonitorListeners() == [serverMonitorListener]
        optionsFromSettings.getUuidRepresentation() == UuidRepresentation.C_SHARP_LEGACY
        optionsFromSettings.getServerApi() == serverApi
    }

    def 'should create settings with SRV protocol'() {
        when:
        MongoClientOptions.builder().build().asMongoClientSettings(null, 'test3.test.build.10gen.cc', SINGLE, null)

        then:
        thrown(IllegalArgumentException)

        when:
        MongoClientOptions.builder().build().asMongoClientSettings([new ServerAddress('host1'), new ServerAddress('host2')],
                'test3.test.build.10gen.cc', MULTIPLE, null)

        then:
        thrown(IllegalArgumentException)

        when:
        MongoClientOptions.builder().build().asMongoClientSettings(null, 'test3.test.build.10gen.cc:27018', MULTIPLE, null)

        then:
        thrown(IllegalArgumentException)

        when:
        MongoClientOptions.builder().build().asMongoClientSettings(null, 'test3.test.build.10gen.cc:27017',
                MULTIPLE, null)

        then:
        thrown(IllegalArgumentException)

        when:
        def settings = MongoClientOptions.builder().build().asMongoClientSettings(null, 'test3.test.build.10gen.cc',
                MULTIPLE, null)

        then:
        settings.clusterSettings == ClusterSettings.builder().srvHost('test3.test.build.10gen.cc').build()
    }

    def 'should be easy to create new options from existing'() {
        when:
        def options = MongoClientOptions.builder()
                .applicationName('appName')
                .readPreference(ReadPreference.secondary())
                .retryReads(true)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .writeConcern(WriteConcern.JOURNALED)
                .minConnectionsPerHost(30)
                .connectionsPerHost(500)
                .connectTimeout(100)
                .socketTimeout(700)
                .serverSelectionTimeout(150)
                .maxWaitTime(200)
                .maxConnectionIdleTime(300)
                .maxConnectionLifeTime(400)
                .maxConnecting(1)
                .sslEnabled(true)
                .sslInvalidHostNameAllowed(true)
                .sslContext(SSLContext.getDefault())
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
                .addConnectionPoolListener(Mock(ConnectionPoolListener))
                .addClusterListener(Mock(ClusterListener))
                .addServerListener(Mock(ServerListener))
                .addServerMonitorListener(Mock(ServerMonitorListener))
                .compressorList([MongoCompressor.createZlibCompressor()])
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace('admin.keys')
                        .kmsProviders(['local': ['key': new byte[64]]])
                        .build())
                .serverApi(ServerApi.builder().version(ServerApiVersion.V1).build())
                .build()

        then:
        expect options, isTheSameAs(MongoClientOptions.builder(options).build())
    }

    def 'applicationName can be 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 126 + '\u00A0'

        when:
        def options = MongoClientOptions.builder().applicationName(applicationName).build()

        then:
        options.applicationName == applicationName
    }

    def 'should throw IllegalArgumentException if applicationName exceeds 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 127 + '\u00A0'

        when:
        MongoClientOptions.builder().applicationName(applicationName)

        then:
        thrown(IllegalArgumentException)
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

    def 'should add connection pool listeners'() {
        given:
        ConnectionPoolListener connectionPoolListenerOne = Mock(ConnectionPoolListener)
        ConnectionPoolListener connectionPoolListenerTwo = Mock(ConnectionPoolListener)
        ConnectionPoolListener connectionPoolListenerThree = Mock(ConnectionPoolListener)

        when:
        def options = MongoClientOptions.builder()
                .build()

        then:
        options.connectionPoolListeners.size() == 0

        when:
        options = MongoClientOptions.builder()
                .addConnectionPoolListener(connectionPoolListenerOne)
                .build()

        then:
        options.connectionPoolListeners.size() == 1
        options.connectionPoolListeners[0].is connectionPoolListenerOne

        when:
        options = MongoClientOptions.builder()
                .addConnectionPoolListener(connectionPoolListenerOne)
                .addConnectionPoolListener(connectionPoolListenerTwo)
                .build()

        then:
        options.connectionPoolListeners.size() == 2
        options.connectionPoolListeners[0].is connectionPoolListenerOne
        options.connectionPoolListeners[1].is connectionPoolListenerTwo

        when:
        def copiedOptions = MongoClientOptions.builder(options).addConnectionPoolListener(connectionPoolListenerThree).build()

        then:
        copiedOptions.connectionPoolListeners.size() == 3
        copiedOptions.connectionPoolListeners[0].is connectionPoolListenerOne
        copiedOptions.connectionPoolListeners[1].is connectionPoolListenerTwo
        copiedOptions.connectionPoolListeners[2].is connectionPoolListenerThree
        options.connectionPoolListeners.size() == 2
        options.connectionPoolListeners[0].is connectionPoolListenerOne
        options.connectionPoolListeners[1].is connectionPoolListenerTwo
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
        def options = MongoClientOptions.builder()
                .applicationName('appName')
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .retryReads(true)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .minConnectionsPerHost(30)
                .connectionsPerHost(500)
                .connectTimeout(100)
                .socketTimeout(700)
                .serverSelectionTimeout(150)
                .maxWaitTime(200)
                .maxConnectionIdleTime(300)
                .maxConnectionLifeTime(400)
                .maxConnecting(1)
                .sslEnabled(true)
                .sslInvalidHostNameAllowed(true)
                .sslContext(SSLContext.getDefault())
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
                .addConnectionPoolListener(Mock(ConnectionPoolListener))
                .addServerListener(Mock(ServerListener))
                .addServerMonitorListener(Mock(ServerMonitorListener))
                .compressorList([MongoCompressor.createZlibCompressor()])
                .autoEncryptionSettings()
                .build()

        when:
        def copy = MongoClientOptions.builder(options).build()

        then:
        copy == options
    }

    def 'should only have the following fields in the builder'() {
        when:
        // A regression test so that if any more methods are added then the builder(final MongoClientOptions options) should be updated
        def actual = MongoClientOptions.Builder.declaredFields.grep { !it.synthetic } *.name.sort()
        def expected = ['applicationName', 'autoEncryptionSettings', 'clusterListeners', 'codecRegistry', 'commandListeners',
                        'compressorList', 'connectTimeout', 'connectionPoolListeners', 'cursorFinalizerEnabled', 'dbDecoderFactory',
                        'dbEncoderFactory', 'heartbeatConnectTimeout', 'heartbeatFrequency', 'heartbeatSocketTimeout', 'localThreshold',
                        'maxConnectionIdleTime', 'maxConnectionLifeTime', 'maxConnectionsPerHost', 'maxConnecting',
                        'maxWaitTime', 'minConnectionsPerHost',
                        'minHeartbeatFrequency', 'readConcern', 'readPreference', 'requiredReplicaSetName', 'retryReads', 'retryWrites',
                        'serverApi', 'serverListeners', 'serverMonitorListeners', 'serverSelectionTimeout', 'serverSelector',
                        'socketTimeout', 'sslContext', 'sslEnabled', 'sslInvalidHostNameAllowed',
                        'uuidRepresentation', 'writeConcern']

        then:
        actual == expected
    }

    def 'should allow 0 (infinite) connectionsPerHost'() {
        expect:
        MongoClientOptions.builder().connectionsPerHost(0).build().getConnectionsPerHost() == 0
    }

    private static class MyDBEncoderFactory implements DBEncoderFactory {
        @Override
        DBEncoder create() {
            new DefaultDBEncoder()
        }
    }
}
