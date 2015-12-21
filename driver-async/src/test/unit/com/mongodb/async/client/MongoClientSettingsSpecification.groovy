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

import com.mongodb.MongoCredential
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.connection.AsynchronousSocketChannelStreamFactoryFactory
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.connection.netty.NettyStreamFactoryFactory
import com.mongodb.event.CommandListener
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSettingsSpecification extends Specification {

    def 'should set the correct default values'() {
        given:
        def options = MongoClientSettings.builder().build()

        expect:
        options.getWriteConcern() == WriteConcern.ACKNOWLEDGED
        options.getReadConcern() == ReadConcern.DEFAULT
        options.getReadPreference() == ReadPreference.primary()
        options.getCommandListeners().isEmpty()
        options.connectionPoolSettings == ConnectionPoolSettings.builder().build()
        options.socketSettings == SocketSettings.builder().build()
        options.heartbeatSocketSettings == SocketSettings.builder().build()
        options.serverSettings == ServerSettings.builder().build()

        System.getProperty('org.mongodb.async.type', 'nio2') == 'netty' ?
        options.streamFactoryFactory instanceof NettyStreamFactoryFactory :
        options.streamFactoryFactory instanceof AsynchronousSocketChannelStreamFactoryFactory
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
        builder.heartbeatSocketSettings(null)
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
    }

    def 'should build with set options'() {
        given:
        def streamFactoryFactory = new NettyStreamFactoryFactory()
        def sslSettings = Stub(SslSettings)
        def socketSettings = Stub(SocketSettings)
        def serverSettings = Stub(ServerSettings)
        def heartbeatSocketSettings = Stub(SocketSettings)
        def credentialList = [MongoCredential.createMongoX509Credential('test')]
        def connectionPoolSettings = Stub(ConnectionPoolSettings)
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def clusterSettings = ClusterSettings.builder().hosts([new ServerAddress('localhost')]).requiredReplicaSetName('test').build()

        def options = MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .readConcern(ReadConcern.LOCAL)
                .addCommandListener(commandListener)
                .sslSettings(sslSettings)
                .socketSettings(socketSettings)
                .serverSettings(serverSettings)
                .heartbeatSocketSettings(heartbeatSocketSettings)
                .credentialList(credentialList)
                .connectionPoolSettings(connectionPoolSettings)
                .codecRegistry(codecRegistry)
                .clusterSettings(clusterSettings)
                                         .streamFactoryFactory(streamFactoryFactory)
                .build()

        expect:
        options.getReadPreference() == ReadPreference.secondary()
        options.getWriteConcern() == WriteConcern.JOURNALED
        options.getReadConcern() == ReadConcern.LOCAL
        options.commandListeners.get(0) == commandListener
        options.connectionPoolSettings == connectionPoolSettings
        options.socketSettings == socketSettings
        options.heartbeatSocketSettings == heartbeatSocketSettings
        options.serverSettings == serverSettings
        options.codecRegistry == codecRegistry
        options.credentialList == credentialList
        options.connectionPoolSettings == connectionPoolSettings
        options.clusterSettings == clusterSettings
        options.streamFactoryFactory == streamFactoryFactory
    }

    def 'should be easy to create new options from existing'() {
        when:
        def sslSettings = Stub(SslSettings)
        def socketSettings = Stub(SocketSettings)
        def serverSettings = Stub(ServerSettings)
        def heartbeatSocketSettings = Stub(SocketSettings)
        def credentialList = [MongoCredential.createMongoX509Credential('test')]
        def connectionPoolSettings = Stub(ConnectionPoolSettings)
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def clusterSettings = ClusterSettings.builder().hosts([new ServerAddress('localhost')]).requiredReplicaSetName('test').build()

        def options = MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .readConcern(ReadConcern.LOCAL)
                .addCommandListener(commandListener)
                .sslSettings(sslSettings)
                .socketSettings(socketSettings)
                .serverSettings(serverSettings)
                .heartbeatSocketSettings(heartbeatSocketSettings)
                .credentialList(credentialList)
                .connectionPoolSettings(connectionPoolSettings)
                .codecRegistry(codecRegistry)
                .clusterSettings(clusterSettings)
                .build()

        then:
        expect options, isTheSameAs(MongoClientSettings.builder(options).build())
    }

    def 'should add command listeners'() {
        given:
        CommandListener commandListenerOne = Mock(CommandListener)
        CommandListener commandListenerTwo = Mock(CommandListener)
        CommandListener commandListenerThree = Mock(CommandListener)

        when:
        def options = MongoClientSettings.builder()
                .build()

        then:
        options.commandListeners.size() == 0

        when:
        options = MongoClientSettings.builder()
                .addCommandListener(commandListenerOne)
                .build()

        then:
        options.commandListeners.size() == 1
        options.commandListeners[0].is commandListenerOne

        when:
        options = MongoClientSettings.builder()
                .addCommandListener(commandListenerOne)
                .addCommandListener(commandListenerTwo)
                .build()

        then:
        options.commandListeners.size() == 2
        options.commandListeners[0].is commandListenerOne
        options.commandListeners[1].is commandListenerTwo

        when:
        def copiedOptions = MongoClientSettings.builder(options).addCommandListener(commandListenerThree).build()

        then:
        copiedOptions.commandListeners.size() == 3
        copiedOptions.commandListeners[0].is commandListenerOne
        copiedOptions.commandListeners[1].is commandListenerTwo
        copiedOptions.commandListeners[2].is commandListenerThree
        options.commandListeners.size() == 2
        options.commandListeners[0].is commandListenerOne
        options.commandListeners[1].is commandListenerTwo
    }


    def 'should only have the following methods in the builder'() {
        when:
        // A regression test so that if anymore methods are added then the builder(final MongoClientSettings settings) should be updated
        def actual = MongoClientSettings.Builder.declaredFields.grep {  !it.synthetic } *.name.sort()
        def expected = ['clusterSettings', 'codecRegistry', 'commandListeners', 'connectionPoolSettings', 'credentialList',
                        'heartbeatSocketSettings', 'readConcern', 'readPreference', 'serverSettings', 'socketSettings', 'sslSettings',
                        'streamFactoryFactory', 'writeConcern']

        then:
        actual == expected
    }
}
