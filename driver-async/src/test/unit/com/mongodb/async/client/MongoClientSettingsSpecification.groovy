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

package com.mongodb.async.client

import com.mongodb.Block
import com.mongodb.ConnectionString
import com.mongodb.MongoCompressor
import com.mongodb.MongoCredential
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.connection.netty.NettyStreamFactoryFactory
import com.mongodb.event.CommandListener
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.isNotAtLeastJava7
import static com.mongodb.CustomMatchers.isTheSameAs
import static java.util.Collections.singletonList
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSettingsSpecification extends Specification {

    def 'should set the correct default values'() {
        given:
        def settings = MongoClientSettings.builder().build()

        expect:
        settings.getWriteConcern() == WriteConcern.ACKNOWLEDGED
        settings.getRetryWrites()
        settings.getReadConcern() == ReadConcern.DEFAULT
        settings.getReadPreference() == ReadPreference.primary()
        settings.getCommandListeners().isEmpty()
        settings.getApplicationName() == null
        settings.getConnectionPoolSettings() == ConnectionPoolSettings.builder().build()
        settings.getSocketSettings() == SocketSettings.builder().build()
        settings.getHeartbeatSocketSettings() == SocketSettings.builder().readTimeout(10000, TimeUnit.MILLISECONDS).build()
        settings.getServerSettings() == ServerSettings.builder().build()
        settings.getStreamFactoryFactory() == null
        settings.getCompressorList() == []
        settings.getCredentialList() == []
        settings.getCredential() == null
    }

    @SuppressWarnings('UnnecessaryObjectReferences')
    def 'should handle illegal arguments'() {
        given:
        def builder = MongoClientSettings.builder()

        when:
        builder.clusterSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.socketSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.connectionPoolSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.serverSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.sslSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.readPreference(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.writeConcern(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.credential(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.credentialList(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.codecRegistry(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.streamFactoryFactory(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.addCommandListener(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.compressorList(null)
        then:
        thrown(IllegalArgumentException)
    }

    def 'should build with supplied settings'() {
        given:
        def streamFactoryFactory = NettyStreamFactoryFactory.builder().build()
        def sslSettings = SslSettings.builder().build()
        def socketSettings = SocketSettings.builder().build()
        def heartbeatSocketSettings = SocketSettings.builder().readTimeout(100, TimeUnit.MILLISECONDS).build()
        def serverSettings = ServerSettings.builder().build()
        def credentialList = [MongoCredential.createMongoX509Credential('test')]
        def connectionPoolSettings = ConnectionPoolSettings.builder().build()
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def clusterSettings = ClusterSettings.builder().hosts([new ServerAddress('localhost')]).requiredReplicaSetName('test').build()

        when:
        def settings = MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .readConcern(ReadConcern.LOCAL)
                .applicationName('app1')
                .addCommandListener(commandListener)
                .codecRegistry(codecRegistry)
                .sslSettings(sslSettings)
                .socketSettings(socketSettings)
                .heartbeatSocketSettings(heartbeatSocketSettings)
                .serverSettings(serverSettings)
                .credentialList(credentialList)
                .connectionPoolSettings(connectionPoolSettings)
                .clusterSettings(clusterSettings).streamFactoryFactory(streamFactoryFactory)
                .compressorList([MongoCompressor.createZlibCompressor()])
                .build()

        then:
        settings.getReadPreference() == ReadPreference.secondary()
        settings.getWriteConcern() == WriteConcern.JOURNALED
        settings.getRetryWrites()
        settings.getReadConcern() == ReadConcern.LOCAL
        settings.getApplicationName() == 'app1'
        settings.getCommandListeners().get(0) == commandListener
        settings.getConnectionPoolSettings() == connectionPoolSettings
        settings.getSocketSettings() == socketSettings
        settings.getHeartbeatSocketSettings() == heartbeatSocketSettings
        settings.getServerSettings() == serverSettings
        settings.getCodecRegistry() == codecRegistry
        settings.getCredentialList() == credentialList
        settings.getCredential() == credentialList.get(0)
        settings.getConnectionPoolSettings() == connectionPoolSettings
        settings.getClusterSettings() == clusterSettings
        settings.getStreamFactoryFactory() == streamFactoryFactory
        settings.getCompressorList() == [MongoCompressor.createZlibCompressor()]
    }

    def 'should create from client settings'() {
        given:
        def streamFactoryFactory = NettyStreamFactoryFactory.builder().build()
        def credential = MongoCredential.createMongoX509Credential('test')
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def clusterSettings = ClusterSettings.builder().hosts([new ServerAddress('localhost')]).requiredReplicaSetName('test').build()

        when:
        def originalSettings = com.mongodb.MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .readConcern(ReadConcern.LOCAL)
                .applicationName('app1')
                .addCommandListener(commandListener)
                .credential(credential)
                .codecRegistry(codecRegistry)
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                    @Override
                    void apply(final ClusterSettings.Builder builder) {
                        builder.applySettings(clusterSettings)
                    }
                })
                .streamFactoryFactory(streamFactoryFactory)
                .compressorList([MongoCompressor.createZlibCompressor()])
                .build()
        def settings = MongoClientSettings.createFromClientSettings(originalSettings)

        then:
        settings.getReadPreference() == ReadPreference.secondary()
        settings.getWriteConcern() == WriteConcern.JOURNALED
        settings.getRetryWrites()
        settings.getReadConcern() == ReadConcern.LOCAL
        settings.getApplicationName() == 'app1'
        settings.getSocketSettings() == SocketSettings.builder().build()
        settings.getHeartbeatSocketSettings() == SocketSettings.builder().readTimeout(10000, TimeUnit.MILLISECONDS).keepAlive(true).build()
        settings.getCommandListeners().get(0) == commandListener
        settings.getCodecRegistry() == codecRegistry
        settings.getCredential() == credential
        settings.getCredentialList() == [credential]
        settings.getClusterSettings() == clusterSettings
        settings.getStreamFactoryFactory() == streamFactoryFactory
        settings.getCompressorList() == [MongoCompressor.createZlibCompressor()]
    }

    def 'should support deprecated multiple credentials'() {
        given:
        def credentialList = [MongoCredential.createMongoX509Credential('test'), MongoCredential.createGSSAPICredential('gssapi')]

        when:
        def settings = MongoClientSettings.builder().credentialList(credentialList).build()

        then:
        settings.getCredentialList() == credentialList

        when:
        settings.getCredential()

        then:
        thrown(IllegalStateException)

        when:
        settings = MongoClientSettings.builder().credential(credentialList.get(0)).build()

        then:
        settings.getCredentialList() == [credentialList.get(0)]
        settings.getCredential() == credentialList.get(0)
    }

    def 'should be easy to create new settings from existing'() {
        when:
        def settings = MongoClientSettings.builder().build()

        then:
        expect settings, isTheSameAs(MongoClientSettings.builder(settings).build())

        when:
        def sslSettings = Stub(SslSettings)
        def socketSettings = SocketSettings.builder().build()
        def serverSettings = ServerSettings.builder().build()
        def heartbeatSocketSettings = SocketSettings.builder().build()
        def credentialList = [MongoCredential.createMongoX509Credential('test')]
        def connectionPoolSettings = ConnectionPoolSettings.builder().build()
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def clusterSettings = ClusterSettings.builder().hosts([new ServerAddress('localhost')]).requiredReplicaSetName('test').build()
        def compressorList = [MongoCompressor.createZlibCompressor()]

        settings = MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .readConcern(ReadConcern.LOCAL)
                .applicationName('app1')
                .addCommandListener(commandListener)
                .sslSettings(sslSettings)
                .socketSettings(socketSettings)
                .serverSettings(serverSettings)
                .heartbeatSocketSettings(heartbeatSocketSettings)
                .credentialList(credentialList)
                .connectionPoolSettings(connectionPoolSettings)
                .codecRegistry(codecRegistry)
                .clusterSettings(clusterSettings)
                .compressorList(compressorList)
                .build()

        then:
        expect settings, isTheSameAs(MongoClientSettings.builder(settings).build())
    }

    def 'applicationName can be 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 126 + '\u00A0'

        when:
        def settings = MongoClientSettings.builder().applicationName(applicationName).build()

        then:
        settings.applicationName == applicationName
    }

    def 'should throw IllegalArgumentException if applicationName exceeds 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 127 + '\u00A0'

        when:
        MongoClientSettings.builder().applicationName(applicationName)

        then:
        thrown(IllegalArgumentException)
    }


    def 'should add command listeners'() {
        given:
        CommandListener commandListenerOne = Mock(CommandListener)
        CommandListener commandListenerTwo = Mock(CommandListener)
        CommandListener commandListenerThree = Mock(CommandListener)

        when:
        def settings = MongoClientSettings.builder().build()

        then:
        settings.commandListeners.size() == 0

        when:
        settings = MongoClientSettings.builder()
                .addCommandListener(commandListenerOne)
                .build()

        then:
        settings.commandListeners.size() == 1
        settings.commandListeners[0].is commandListenerOne

        when:
        settings = MongoClientSettings.builder()
                .addCommandListener(commandListenerOne)
                .addCommandListener(commandListenerTwo)
                .build()

        then:
        settings.commandListeners.size() == 2
        settings.commandListeners[0].is commandListenerOne
        settings.commandListeners[1].is commandListenerTwo

        when:
        def copiedsettings = MongoClientSettings.builder(settings).addCommandListener(commandListenerThree).build()

        then:
        copiedsettings.commandListeners.size() == 3
        copiedsettings.commandListeners[0].is commandListenerOne
        copiedsettings.commandListeners[1].is commandListenerTwo
        copiedsettings.commandListeners[2].is commandListenerThree
        settings.commandListeners.size() == 2
        settings.commandListeners[0].is commandListenerOne
        settings.commandListeners[1].is commandListenerTwo
    }

    @IgnoreIf({ isNotAtLeastJava7() })
    def 'should build settings from a connection string'() {
        when:
        ConnectionString connectionString = new ConnectionString('mongodb://user:pass@host1:1,host2:2/'
                + '?authMechanism=SCRAM-SHA-1&authSource=test'
                + '&minPoolSize=5&maxPoolSize=10&waitQueueMultiple=7'
                + '&waitQueueTimeoutMS=150&maxIdleTimeMS=200&maxLifeTimeMS=300'
                + '&connectTimeoutMS=2500'
                + '&socketTimeoutMS=5500'
                + '&serverSelectionTimeoutMS=25000'
                + '&localThresholdMS=30'
                + '&heartbeatFrequencyMS=20000'
                + '&appName=MyApp'
                + '&replicaSet=test'
                + '&retryWrites=true'
                + '&ssl=true&sslInvalidHostNameAllowed=true'
                + '&w=majority&wTimeoutMS=2500'
                + '&readPreference=secondary'
                + '&readConcernLevel=majority'
                + '&compressors=zlib&zlibCompressionLevel=5'
        )
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build()
        MongoClientSettings expected = MongoClientSettings.builder()
                .clusterSettings(ClusterSettings.builder().applyConnectionString(connectionString).build())
                .heartbeatSocketSettings(SocketSettings.builder()
                    .connectTimeout(connectionString.getConnectTimeout(), TimeUnit.MILLISECONDS)
                    .readTimeout(connectionString.getConnectTimeout(), TimeUnit.MILLISECONDS).build())
                .connectionPoolSettings(ConnectionPoolSettings.builder().applyConnectionString(connectionString).build())
                .serverSettings(ServerSettings.builder().applyConnectionString(connectionString).build())
                .socketSettings(SocketSettings.builder().applyConnectionString(connectionString).build())
                .sslSettings(SslSettings.builder().applyConnectionString(connectionString).build())
                .readConcern(ReadConcern.MAJORITY)
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(2500, TimeUnit.MILLISECONDS))
                .applicationName('MyApp')
                .credential(MongoCredential.createScramSha1Credential('user', 'test', 'pass'.toCharArray()))
                .compressorList([MongoCompressor.createZlibCompressor().withProperty(MongoCompressor.LEVEL, 5)])
                .retryWrites(true)
                .build()

        then:
        expect expected, isTheSameAs(settings, ['heartbeatSocketSettings'])
        settings.getHeartbeatSocketSettings() == expected.getHeartbeatSocketSettings()
    }

    @IgnoreIf({ isNotAtLeastJava7() })
    def 'should allow easy configuration of nested settings'() {
        when:
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
            @Override
            void apply(final ClusterSettings.Builder builder) {
                builder.description('My Cluster').hosts(singletonList(new ServerAddress()))
            }
        })
                .applyToConnectionPoolSettings(new Block<ConnectionPoolSettings.Builder>() {
            @Override
            void apply(final ConnectionPoolSettings.Builder builder) {
                builder.maxWaitQueueSize(22)
            }
        })
                .applyToServerSettings(new Block<ServerSettings.Builder>() {
            @Override
            void apply(final ServerSettings.Builder builder) {
                builder.heartbeatFrequency(10, TimeUnit.SECONDS)
            }
        })
                .applyToSocketSettings(new Block<SocketSettings.Builder>() {
            @Override
            void apply(final SocketSettings.Builder builder) {
                builder.sendBufferSize(99)
            }
        })
                .applyToSslSettings(new Block<SslSettings.Builder>() {
            @Override
            void apply(final SslSettings.Builder builder) {
                builder.enabled(true).invalidHostNameAllowed(true)
            }
        })
                .heartbeatSocketSettings(SocketSettings.builder().receiveBufferSize(99).readTimeout(1, TimeUnit.SECONDS).build())
        .build()

        MongoClientSettings expected = MongoClientSettings.builder()
                .clusterSettings(ClusterSettings.builder().description('My Cluster').hosts(singletonList(new ServerAddress())).build())
                .connectionPoolSettings(ConnectionPoolSettings.builder().maxWaitQueueSize(22).build())
                .heartbeatSocketSettings(SocketSettings.builder().readTimeout(1, TimeUnit.SECONDS).receiveBufferSize(99).build())
                .serverSettings(ServerSettings.builder().heartbeatFrequency(10, TimeUnit.SECONDS).build())
                .socketSettings(SocketSettings.builder().sendBufferSize(99).build())
                .sslSettings(SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build())
                .build()

        then:
        expect expected, isTheSameAs(settings)
    }

    def 'should use the socket settings connectionTime out for the heartbeat settings'() {
        when:
        def settings = MongoClientSettings.builder().applyToSocketSettings(new Block<SocketSettings.Builder>() {
            @Override
            void apply(final SocketSettings.Builder builder) {
                builder.connectTimeout(42, TimeUnit.SECONDS).readTimeout(60, TimeUnit.SECONDS).receiveBufferSize(22).sendBufferSize(10)
            }
        }).build()
        def expected = SocketSettings.builder().connectTimeout(42, TimeUnit.SECONDS).readTimeout(42, TimeUnit.SECONDS).build()

        then:
        settings.getHeartbeatSocketSettings() == expected
    }

    def 'should only have the following methods in the builder'() {
        when:
        // A regression test so that if anymore methods are added then the builder(final MongoClientSettings settings) should be updated
        def extras = ['credentialList', 'clusterSettings', 'connectionPoolSettings', 'heartbeatSocketSettings', 'serverSettings',
                      'socketSettings' , 'sslSettings']
        def actual = MongoClientSettings.Builder.declaredMethods.grep { !it.synthetic } *.name.sort() - extras
        def expected = com.mongodb.MongoClientSettings.Builder.declaredMethods.grep { !it.synthetic } *.name.sort() - 'commandListenerList'

        then:
        actual == expected
    }
}
