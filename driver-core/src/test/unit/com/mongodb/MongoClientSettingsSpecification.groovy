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

import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.connection.netty.NettyStreamFactoryFactory
import com.mongodb.event.CommandListener
import org.bson.UuidRepresentation
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSettingsSpecification extends Specification {

    def 'should set the correct default values'() {
        given:
        def settings = MongoClientSettings.builder().build()

        expect:
        settings.getWriteConcern() == WriteConcern.ACKNOWLEDGED
        settings.getRetryWrites()
        settings.getRetryReads()
        settings.getReadConcern() == ReadConcern.DEFAULT
        settings.getReadPreference() == ReadPreference.primary()
        settings.getCommandListeners().isEmpty()
        settings.getApplicationName() == null
        settings.clusterSettings == ClusterSettings.builder().build()
        settings.connectionPoolSettings == ConnectionPoolSettings.builder().build()
        settings.socketSettings == SocketSettings.builder().build()
        settings.heartbeatSocketSettings == SocketSettings.builder().readTimeout(10000, TimeUnit.MILLISECONDS).build()
        settings.serverSettings == ServerSettings.builder().build()
        settings.streamFactoryFactory == null
        settings.compressorList == []
        settings.credential == null
        settings.uuidRepresentation == UuidRepresentation.UNSPECIFIED
    }

    @SuppressWarnings('UnnecessaryObjectReferences')
    def 'should handle illegal arguments'() {
        given:
        def builder = MongoClientSettings.builder()

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
        builder.credential(null)
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

        when:
        builder.uuidRepresentation(null)
        then:
        thrown(IllegalArgumentException)
    }

    def 'should build with set configuration'() {
        given:
        def streamFactoryFactory = NettyStreamFactoryFactory.builder().build()
        def credential = MongoCredential.createMongoX509Credential('test')
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def clusterSettings = ClusterSettings.builder().hosts([new ServerAddress('localhost')]).requiredReplicaSetName('test').build()

        when:
        def settings = MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .retryReads(true)
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
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build()

        then:
        settings.getReadPreference() == ReadPreference.secondary()
        settings.getWriteConcern() == WriteConcern.JOURNALED
        settings.getRetryWrites()
        settings.getRetryReads()
        settings.getReadConcern() == ReadConcern.LOCAL
        settings.getApplicationName() == 'app1'
        settings.getSocketSettings() == SocketSettings.builder().build()
        settings.getHeartbeatSocketSettings() == SocketSettings.builder().readTimeout(10000, TimeUnit.MILLISECONDS).build()
        settings.getCommandListeners().get(0) == commandListener
        settings.getCodecRegistry() == codecRegistry
        settings.getCredential() == credential
        settings.getClusterSettings() == clusterSettings
        settings.getStreamFactoryFactory() == streamFactoryFactory
        settings.getCompressorList() == [MongoCompressor.createZlibCompressor()]
        settings.getUuidRepresentation() == UuidRepresentation.STANDARD
    }

    def 'should be easy to create new settings from existing'() {
        when:
        def settings = MongoClientSettings.builder().build()

        then:
        expect settings, isTheSameAs(MongoClientSettings.builder(settings).build())

        when:
        def credential = MongoCredential.createMongoX509Credential('test')
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def compressorList = [MongoCompressor.createZlibCompressor()]

        settings = MongoClientSettings.builder()
                .heartbeatConnectTimeoutMS(24000)
                .heartbeatSocketTimeoutMS(12000)
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .retryReads(false)
                .readConcern(ReadConcern.LOCAL)
                .applicationName('app1')
                .addCommandListener(commandListener)
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                    @Override
                    void apply(final ClusterSettings.Builder builder) {
                           builder.hosts([new ServerAddress('localhost')])
                                   .requiredReplicaSetName('test')
                    }
                })
                .credential(credential)
                .codecRegistry(codecRegistry)
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
        def settings = MongoClientSettings.builder()
                .build()

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
        def copied = MongoClientSettings.builder(settings).addCommandListener(commandListenerThree).build()

        then:
        copied.commandListeners.size() == 3
        copied.commandListeners[0].is commandListenerOne
        copied.commandListeners[1].is commandListenerTwo
        copied.commandListeners[2].is commandListenerThree
        settings.commandListeners.size() == 2
        settings.commandListeners[0].is commandListenerOne
        settings.commandListeners[1].is commandListenerTwo
    }

    def 'should build settings from a connection string'() {
        when:
        ConnectionString connectionString = new ConnectionString('mongodb://user:pass@host1:1,host2:2/'
                + '?authMechanism=SCRAM-SHA-1&authSource=test'
                + '&minPoolSize=5&maxPoolSize=10'
                + '&waitQueueTimeoutMS=150&maxIdleTimeMS=200&maxLifeTimeMS=300'
                + '&connectTimeoutMS=2500'
                + '&socketTimeoutMS=5500'
                + '&serverSelectionTimeoutMS=25000'
                + '&localThresholdMS=30'
                + '&heartbeatFrequencyMS=20000'
                + '&appName=MyApp'
                + '&replicaSet=test'
                + '&retryWrites=true'
                + '&retryReads=true'
                + '&ssl=true&sslInvalidHostNameAllowed=true'
                + '&w=majority&wTimeoutMS=2500'
                + '&readPreference=secondary'
                + '&readConcernLevel=majority'
                + '&compressors=zlib&zlibCompressionLevel=5'
                + '&uuidRepresentation=standard'
        )
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build()
        MongoClientSettings expected = MongoClientSettings.builder()
            .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
            @Override
            void apply(final ClusterSettings.Builder builder) {
                builder.hosts([new ServerAddress('host1', 1), new ServerAddress('host2', 2)])
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredReplicaSetName('test')
                        .serverSelectionTimeout(25000, TimeUnit.MILLISECONDS)
                        .localThreshold(30, TimeUnit.MILLISECONDS)
            }
        })
            .applyToConnectionPoolSettings(new Block<ConnectionPoolSettings.Builder>() {
            @Override
            void apply(final ConnectionPoolSettings.Builder builder) {
                builder.minSize(5)
                        .maxSize(10)
                        .maxWaitTime(150, TimeUnit.MILLISECONDS)
                        .maxConnectionLifeTime(300, TimeUnit.MILLISECONDS)
                        .maxConnectionIdleTime(200, TimeUnit.MILLISECONDS)
            }
        })
            .applyToServerSettings(new Block<ServerSettings.Builder>() {
            @Override
            void apply(final ServerSettings.Builder builder) {
                builder.heartbeatFrequency(20000, TimeUnit.MILLISECONDS)
            }
        })
            .applyToSocketSettings(new Block<SocketSettings.Builder>() {
            @Override
            void apply(final SocketSettings.Builder builder) {
                builder.connectTimeout(2500, TimeUnit.MILLISECONDS)
                        .readTimeout(5500, TimeUnit.MILLISECONDS)
            }
        })
            .applyToSslSettings(new Block<SslSettings.Builder>() {
            @Override
            void apply(final SslSettings.Builder builder) {
                builder.enabled(true)
                        .invalidHostNameAllowed(true)
            }
        })
            .readConcern(ReadConcern.MAJORITY)
            .readPreference(ReadPreference.secondary())
            .writeConcern(WriteConcern.MAJORITY.withWTimeout(2500, TimeUnit.MILLISECONDS))
            .applicationName('MyApp')
            .credential(MongoCredential.createScramSha1Credential('user', 'test', 'pass'.toCharArray()))
            .compressorList([MongoCompressor.createZlibCompressor().withProperty(MongoCompressor.LEVEL, 5)])
            .retryWrites(true)
            .retryReads(true)
            .uuidRepresentation(UuidRepresentation.STANDARD)
            .build()

        then:
        expect expected, isTheSameAs(settings)
    }

    def 'should build settings from a connection string with default values'() {
        when:
        def builder = MongoClientSettings.builder()
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
            @Override
            void apply(final ClusterSettings.Builder builder) {
                builder.hosts([new ServerAddress('localhost', 27017)])
                        .mode(ClusterConnectionMode.SINGLE)
                        .serverSelectionTimeout(25000, TimeUnit.MILLISECONDS)
                        .localThreshold(30, TimeUnit.MILLISECONDS)
            }
        })
                .applyToConnectionPoolSettings(new Block<ConnectionPoolSettings.Builder>() {
            @Override
            void apply(final ConnectionPoolSettings.Builder builder) {
                builder.minSize(5)
                        .maxSize(10)
                        .maxWaitTime(150, TimeUnit.MILLISECONDS)
                        .maxConnectionLifeTime(300, TimeUnit.MILLISECONDS)
                        .maxConnectionIdleTime(200, TimeUnit.MILLISECONDS)
            }
        })
                .applyToServerSettings(new Block<ServerSettings.Builder>() {
            @Override
            void apply(final ServerSettings.Builder builder) {
                builder.heartbeatFrequency(20000, TimeUnit.MILLISECONDS)
            }
        })
                .applyToSocketSettings(new Block<SocketSettings.Builder>() {
            @Override
            void apply(final SocketSettings.Builder builder) {
                builder.connectTimeout(2500, TimeUnit.MILLISECONDS)
                        .readTimeout(5500, TimeUnit.MILLISECONDS)
            }
        })
                .applyToSslSettings(new Block<SslSettings.Builder>() {
            @Override
            void apply(final SslSettings.Builder builder) {
                builder.enabled(true)
                        .invalidHostNameAllowed(true)
            }
        })
                .readConcern(ReadConcern.MAJORITY)
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(2500, TimeUnit.MILLISECONDS))
                .applicationName('MyApp')
                .credential(MongoCredential.createScramSha1Credential('user', 'test', 'pass'.toCharArray()))
                .compressorList([MongoCompressor.createZlibCompressor().withProperty(MongoCompressor.LEVEL, 5)])
                .retryWrites(true)
                .retryReads(true)

        def expectedSettings = builder.build()
        def settingsWithDefaultConnectionStringApplied = builder
                .applyConnectionString(new ConnectionString('mongodb://localhost'))
                .build()

        then:
        expect expectedSettings, isTheSameAs(settingsWithDefaultConnectionStringApplied)
    }

    def 'should use the socket settings connectionTimeout for the heartbeat settings'() {
        when:
        def settings = MongoClientSettings.builder().applyToSocketSettings { SocketSettings.Builder builder ->
            builder.connectTimeout(42, TimeUnit.SECONDS).readTimeout(60, TimeUnit.SECONDS)
                    .receiveBufferSize(22).sendBufferSize(10)
        }.build()

        then:
        settings.getHeartbeatSocketSettings() == SocketSettings.builder().connectTimeout(42, TimeUnit.SECONDS)
                .readTimeout(42, TimeUnit.SECONDS)
                .build()

        when:
        settings = MongoClientSettings.builder(settings)
                .applyToSocketSettings { SocketSettings.Builder builder ->
                    builder.connectTimeout(21, TimeUnit.SECONDS)
                }.build()

        then:
        settings.getHeartbeatSocketSettings() == SocketSettings.builder().connectTimeout(21, TimeUnit.SECONDS)
                .readTimeout(21, TimeUnit.SECONDS)
                .build()
    }

    def 'should use the configured heartbeat timeouts for the heartbeat settings'() {
        when:
        def settings = MongoClientSettings.builder()
                .heartbeatConnectTimeoutMS(24000)
                .heartbeatSocketTimeoutMS(12000)
                .applyToSocketSettings { SocketSettings.Builder builder ->
                    builder.connectTimeout(42, TimeUnit.SECONDS).readTimeout(60, TimeUnit.SECONDS)
                            .receiveBufferSize(22).sendBufferSize(10)
                }.build()
        then:
        settings.getHeartbeatSocketSettings() == SocketSettings.builder().connectTimeout(24, TimeUnit.SECONDS)
                .readTimeout(12, TimeUnit.SECONDS).build()

        when:
        settings = MongoClientSettings.builder(settings)
                .applyToSocketSettings { SocketSettings.Builder builder ->
                    builder.connectTimeout(21, TimeUnit.SECONDS)
                }.build()

        then:
        settings.getHeartbeatSocketSettings() == SocketSettings.builder().connectTimeout(24, TimeUnit.SECONDS)
                .readTimeout(12, TimeUnit.SECONDS)
                .build()
    }

    def 'should only have the following fields in the builder'() {
        when:
        // A regression test so that if anymore fields are added then the builder(final MongoClientSettings settings) should be updated
        def actual = MongoClientSettings.Builder.declaredFields.grep {  !it.synthetic } *.name.sort()
        def expected = ['applicationName', 'autoEncryptionSettings', 'clusterSettingsBuilder', 'codecRegistry', 'commandListeners',
                        'compressorList', 'connectionPoolSettingsBuilder', 'credential',
                        'heartbeatConnectTimeoutMS', 'heartbeatSocketTimeoutMS',
                        'readConcern', 'readPreference', 'retryReads',
                        'retryWrites', 'serverSettingsBuilder', 'socketSettingsBuilder', 'sslSettingsBuilder', 'streamFactoryFactory',
                        'uuidRepresentation', 'writeConcern']

        then:
        actual == expected
    }

    def 'should only have the following methods in the builder'() {
        when:
        // A regression test so that if anymore methods are added then the builder(final MongoClientSettings settings) should be updated
        def actual = MongoClientSettings.Builder.declaredMethods.grep {  !it.synthetic } *.name.sort()
        def expected = ['addCommandListener', 'applicationName', 'applyConnectionString', 'applyToClusterSettings',
                        'applyToConnectionPoolSettings', 'applyToServerSettings', 'applyToSocketSettings', 'applyToSslSettings',
                        'autoEncryptionSettings', 'build', 'codecRegistry', 'commandListenerList', 'compressorList', 'credential',
                        'heartbeatConnectTimeoutMS', 'heartbeatSocketTimeoutMS', 'readConcern', 'readPreference', 'retryReads',
                        'retryWrites', 'streamFactoryFactory', 'uuidRepresentation',
                        'writeConcern']
        then:
        actual == expected
    }
}
