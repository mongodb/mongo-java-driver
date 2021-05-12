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

import com.mongodb.AuthenticationMechanism
import com.mongodb.MongoCompressor
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.internal.async.SingleResultCallback
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.internal.Base64
import spock.lang.Specification

import java.nio.charset.Charset
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.MongoCredential.createMongoX509Credential
import static com.mongodb.MongoCredential.createPlainCredential
import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.MongoCredential.createScramSha256Credential
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.internal.connection.ClientMetadataHelperSpecification.createExpectedClientMetadataDocument
import static com.mongodb.internal.connection.MessageHelper.buildSuccessfulReply
import static com.mongodb.internal.connection.MessageHelper.decodeCommand

class InternalStreamConnectionInitializerSpecification extends Specification {

    def serverId = new ServerId(new ClusterId(), new ServerAddress())
    def internalConnection = new TestInternalConnection(serverId)

    def 'should create correct description'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(SINGLE, null, null, [], null)

        when:
        enqueueSuccessfulReplies(false, null)
        def description = initializer.startHandshake(internalConnection)
        description = initializer.finishHandshake(internalConnection, description)
        def connectionDescription = description.connectionDescription
        def serverDescription = description.serverDescription

        then:
        connectionDescription == getExpectedConnectionDescription(connectionDescription.connectionId.localValue, null)
        serverDescription == getExpectedServerDescription(serverDescription)
    }

    def 'should create correct description asynchronously'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(SINGLE, null, null, [], null)

        when:
        enqueueSuccessfulReplies(false, null)
        def futureCallback = new FutureResultCallback<InternalConnectionInitializationDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()
        def connectionDescription = description.connectionDescription
        def serverDescription = description.serverDescription

        then:
        connectionDescription == getExpectedConnectionDescription(connectionDescription.connectionId.localValue, null)
        serverDescription == getExpectedServerDescription(serverDescription)
    }

    def 'should create correct description with server connection id'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(SINGLE, null, null, [], null)

        when:
        enqueueSuccessfulReplies(false, 123)
        def internalDescription = initializer.startHandshake(internalConnection)
        def connectionDescription = initializer.finishHandshake(internalConnection, internalDescription).connectionDescription

        then:
        connectionDescription == getExpectedConnectionDescription(connectionDescription.connectionId.localValue, 123)
    }

    def 'should create correct description with server connection id from isMaster'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(SINGLE, null, null, [], null)

        when:
        enqueueSuccessfulRepliesWithConnectionIdIsIsMasterResponse(false, 123)
        def internalDescription = initializer.startHandshake(internalConnection)
        def connectionDescription = initializer.finishHandshake(internalConnection, internalDescription).connectionDescription

        then:
        connectionDescription == getExpectedConnectionDescription(connectionDescription.connectionId.localValue, 123)
    }

    def 'should create correct description with server connection id asynchronously'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(SINGLE, null, null, [], null)

        when:
        enqueueSuccessfulReplies(false, 123)
        def futureCallback = new FutureResultCallback<InternalConnectionInitializationDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get().connectionDescription

        then:
        description == getExpectedConnectionDescription(description.connectionId.localValue, 123)
    }

    def 'should create correct description with server connection id from isMaster asynchronously'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(SINGLE, null, null, [], null)

        when:
        enqueueSuccessfulRepliesWithConnectionIdIsIsMasterResponse(false, 123)
        def futureCallback = new FutureResultCallback<InternalConnectionInitializationDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get().connectionDescription

        then:
        description == getExpectedConnectionDescription(description.connectionId.localValue, 123)
    }

    def 'should authenticate'() {
        given:
        def firstAuthenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer(SINGLE, firstAuthenticator, null, [], null)

        when:
        enqueueSuccessfulReplies(false, null)

        def internalDescription = initializer.startHandshake(internalConnection)
        def connectionDescription = initializer.finishHandshake(internalConnection, internalDescription).connectionDescription

        then:
        connectionDescription
        1 * firstAuthenticator.authenticate(internalConnection, _)
    }

    def 'should authenticate asynchronously'() {
        given:
        def authenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator, null, [], null)

        when:
        enqueueSuccessfulReplies(false, null)

        def futureCallback = new FutureResultCallback<InternalConnectionInitializationDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get().connectionDescription

        then:
        description
        1 * authenticator.authenticateAsync(internalConnection, _, _) >> { it[2].onResult(null, null) }
    }

    def 'should not authenticate if server is an arbiter'() {
        given:
        def authenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator, null, [], null)

        when:
        enqueueSuccessfulReplies(true, null)

        def internalDescription = initializer.startHandshake(internalConnection)
        def connectionDescription = initializer.finishHandshake(internalConnection, internalDescription).connectionDescription

        then:
        connectionDescription
        0 * authenticator.authenticate(internalConnection, _)
    }

    def 'should not authenticate asynchronously if server is an arbiter asynchronously'() {
        given:
        def authenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator, null, [], null)

        when:
        enqueueSuccessfulReplies(true, null)

        def futureCallback = new FutureResultCallback<InternalConnectionInitializationDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get().connectionDescription

        then:
        description
        0 * authenticator.authenticateAsync(internalConnection, _, _)
    }

    def 'should add client metadata document to isMaster command'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(SINGLE, null, clientMetadataDocument, [], null)
        def expectedIsMasterCommandDocument = new BsonDocument('ismaster', new BsonInt32(1)).append('helloOk', BsonBoolean.TRUE)
        if (clientMetadataDocument != null) {
            expectedIsMasterCommandDocument.append('client', clientMetadataDocument)
        }

        when:
        enqueueSuccessfulReplies(false, null)
        if (async) {
            def latch = new CountDownLatch(1)
            def callback = { result, t -> latch.countDown() } as SingleResultCallback
            initializer.initializeAsync(internalConnection, callback)
            latch.await()
        } else {
            def internalDescription = initializer.startHandshake(internalConnection)
            initializer.finishHandshake(internalConnection, internalDescription)
        }

        then:
        decodeCommand(internalConnection.getSent()[0]) == expectedIsMasterCommandDocument

        where:
        [clientMetadataDocument, async] << [[createExpectedClientMetadataDocument('appName'), null],
                                            [true, false]].combinations()
    }

    def 'should add compression to isMaster command'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(SINGLE, null, null, compressors, null)
        def expectedIsMasterCommandDocument = new BsonDocument('ismaster', new BsonInt32(1)).append('helloOk', BsonBoolean.TRUE)

        def compressionArray = new BsonArray()
        for (def compressor : compressors) {
            compressionArray.add(new BsonString(compressor.getName()))
        }
        if (!compressionArray.isEmpty()) {
            expectedIsMasterCommandDocument.append('compression', compressionArray)
        }

        when:
        enqueueSuccessfulReplies(false, null)
        if (async) {
            def latch = new CountDownLatch(1)
            def callback = { result, t -> latch.countDown() } as SingleResultCallback
            initializer.initializeAsync(internalConnection, callback)
            latch.await()
        } else {
            def internalDescription = initializer.startHandshake(internalConnection)
            initializer.finishHandshake(internalConnection, internalDescription)
        }

        then:
        decodeCommand(internalConnection.getSent()[0]) == expectedIsMasterCommandDocument

        where:
        [compressors, async] << [[[], [MongoCompressor.createZlibCompressor()]],
                                 [true, false]].combinations()
    }

    def 'should speculatively authenticate with default authenticator'() {
        given:
        def credential = new MongoCredentialWithCache(createCredential('user', 'database', 'pencil' as char[]))
        def authenticator = Spy(DefaultAuthenticator, constructorArgs: [credential, null])
        def scramShaAuthenticator = Spy(ScramShaAuthenticator,
                constructorArgs: [credential.withMechanism(AuthenticationMechanism.SCRAM_SHA_256),
                                  { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' }, null])
        def initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator, null, [], null)
        authenticator.getAuthenticatorForIsMaster() >> scramShaAuthenticator
        def serverResponse = 'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
        def speculativeAuthenticateResponse =
                BsonDocument.parse("{ conversationId: 1, payload: BinData(0, '${encode64(serverResponse)}'), done: false }")
        def firstClientChallenge = 'n,,n=user,r=rOprNGfwEbeRWgbNEkqO'

        when:
        enqueueSpeculativeAuthenticationResponsesForScramSha256()
        def description = initializeConnection(async, initializer, internalConnection)

        then:
        description
        if (async) {
            1 * scramShaAuthenticator.authenticateAsync(internalConnection, _, _)
        } else {
            1 * scramShaAuthenticator.authenticate(internalConnection, _)
        }
        1 * ((SpeculativeAuthenticator) scramShaAuthenticator).createSpeculativeAuthenticateCommand(_)
        ((SpeculativeAuthenticator) scramShaAuthenticator).getSpeculativeAuthenticateResponse() == speculativeAuthenticateResponse
        def expectedIsMasterCommand = createIsMasterCommand(firstClientChallenge, 'SCRAM-SHA-256', true)
        expectedIsMasterCommand == decodeCommand(internalConnection.getSent()[0])

        where:
        async << [false, false]
    }

    def 'should speculatively authenticate with SCRAM-SHA-256 authenticator'() {
        given:
        def credential = new MongoCredentialWithCache(createScramSha256Credential('user', 'database', 'pencil' as char[]))
        def authenticator = Spy(ScramShaAuthenticator, constructorArgs: [credential, { 'rOprNGfwEbeRWgbNEkqO' }, { 'pencil' }, null])
        def initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator, null, [], null)
        def serverResponse = 'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
        def speculativeAuthenticateResponse =
                BsonDocument.parse("{ conversationId: 1, payload: BinData(0, '${encode64(serverResponse)}'), done: false }")
        def firstClientChallenge = 'n,,n=user,r=rOprNGfwEbeRWgbNEkqO'

        when:
        enqueueSpeculativeAuthenticationResponsesForScramSha256()
        def description = initializeConnection(async, initializer, internalConnection)

        then:
        description
        if (async) {
            1 * authenticator.authenticateAsync(internalConnection, _, _)
        } else {
            1 * authenticator.authenticate(internalConnection, _)
        }
        1 * ((SpeculativeAuthenticator) authenticator).createSpeculativeAuthenticateCommand(_)
        ((SpeculativeAuthenticator) authenticator).getSpeculativeAuthenticateResponse() == speculativeAuthenticateResponse
        def expectedIsMasterCommand = createIsMasterCommand(firstClientChallenge, 'SCRAM-SHA-256', false)
        expectedIsMasterCommand == decodeCommand(internalConnection.getSent()[0])

        where:
        async << [true, false]
    }

    def 'should speculatively authenticate with SCRAM-SHA-1 authenticator'() {
        given:
        def credential = new MongoCredentialWithCache(createScramSha1Credential('user', 'database', 'pencil' as char[]))
        def authenticator = Spy(ScramShaAuthenticator, constructorArgs: [credential, { 'fyko+d2lbbFgONRv9qkxdawL' }, { 'pencil' }, null])
        def initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator, null, [], null)
        def serverResponse = 'r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096'
        def speculativeAuthenticateResponse =
                BsonDocument.parse("{ conversationId: 1, payload: BinData(0, '${encode64(serverResponse)}'), done: false }")

        when:
        enqueueSpeculativeAuthenticationResponsesForScramSha1()
        def description = initializeConnection(async, initializer, internalConnection)
        def firstClientChallenge = 'n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL'

        then:
        description
        if (async) {
            1 * authenticator.authenticateAsync(internalConnection, _, _)
        } else {
            1 * authenticator.authenticate(internalConnection, _)
        }
        1 * ((SpeculativeAuthenticator) authenticator).createSpeculativeAuthenticateCommand(_)
        ((SpeculativeAuthenticator) authenticator).getSpeculativeAuthenticateResponse() == speculativeAuthenticateResponse
        def expectedIsMasterCommand = createIsMasterCommand(firstClientChallenge, 'SCRAM-SHA-1', false)
        expectedIsMasterCommand == decodeCommand(internalConnection.getSent()[0])

        where:
        async << [true, false]
    }

    def 'should speculatively authenticate with X509 authenticator'() {
        given:
        def credential = new MongoCredentialWithCache(createMongoX509Credential())
        def authenticator = Spy(X509Authenticator, constructorArgs: [credential, null])
        def initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator, null, [], null)
        def speculativeAuthenticateResponse =
                BsonDocument.parse('{ dbname: "$external", user: "CN=client,OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US"}')

        when:
        enqueueSpeculativeAuthenticationResponsesForX509()
        def description = initializeConnection(async, initializer, internalConnection)

        then:
        description
        if (async) {
            1 * authenticator.authenticateAsync(internalConnection, _, _)
        } else {
            1 * authenticator.authenticate(internalConnection, _)
        }
        1 * ((SpeculativeAuthenticator) authenticator).createSpeculativeAuthenticateCommand(_)
        ((SpeculativeAuthenticator) authenticator).getSpeculativeAuthenticateResponse() == speculativeAuthenticateResponse
        def expectedIsMasterCommand = createIsMasterCommand('', 'MONGODB-X509', false)
        expectedIsMasterCommand == decodeCommand(internalConnection.getSent()[0])

        where:
        async << [true, false]
    }

    def 'should not speculatively authenticate with Plain authenticator'() {
        given:
        def credential = new MongoCredentialWithCache(createPlainCredential('user', 'database', 'pencil' as char[]))
        def authenticator = Spy(PlainAuthenticator, constructorArgs: [credential, null])
        def initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator, null, [], null)

        when:
        enqueueSpeculativeAuthenticationResponsesForPlain()
        initializeConnection(async, initializer, internalConnection)

        then:
        ((SpeculativeAuthenticator) authenticator).getSpeculativeAuthenticateResponse() == null
        ((SpeculativeAuthenticator) authenticator)
                .createSpeculativeAuthenticateCommand(internalConnection) == null
        BsonDocument.parse('{ismaster: 1, helloOk: true}') == decodeCommand(internalConnection.getSent()[0])

        where:
        async << [true, false]
    }

    private ConnectionDescription getExpectedConnectionDescription(final Integer localValue, final Integer serverValue) {
        new ConnectionDescription(new ConnectionId(serverId, localValue, serverValue),
                3, ServerType.STANDALONE, 512, 16777216, 33554432, [])
    }

    def initializeConnection(final boolean async, final InternalStreamConnectionInitializer initializer,
                             final TestInternalConnection connection) {
        if (async) {
            def futureCallback = new FutureResultCallback<ConnectionDescription>()
            initializer.initializeAsync(connection, futureCallback)
            futureCallback.get()
        } else {
            def internalDescription = initializer.startHandshake(connection)
            initializer.finishHandshake(connection, internalDescription)
        }
    }

    private ServerDescription getExpectedServerDescription(ServerDescription actualServerDescription) {
        ServerDescription.builder()
                .ok(true)
                .address(serverId.address)
                .type(ServerType.STANDALONE)
                .state(ServerConnectionState.CONNECTED)
                .minWireVersion(0)
                .maxWireVersion(3)
                .maxDocumentSize(16777216)
                .roundTripTime(actualServerDescription.getRoundTripTimeNanos(), TimeUnit.NANOSECONDS)
                .lastUpdateTimeNanos(actualServerDescription.getLastUpdateTime(TimeUnit.NANOSECONDS))
                .build()
    }

    def enqueueSuccessfulReplies(final boolean isArbiter, final Integer serverConnectionId) {
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1, ' +
                'maxWireVersion: 3' +
                (isArbiter ? ', isreplicaset: true, arbiterOnly: true' : '') +
                '}'))
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1 ' +
                (serverConnectionId == null ? '' : ', connectionId: ' + serverConnectionId) +
                '}'))
    }

    def enqueueSuccessfulRepliesWithConnectionIdIsIsMasterResponse(final boolean isArbiter, final Integer serverConnectionId) {
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1, ' +
                        'maxWireVersion: 3,' +
                        'connectionId: ' + serverConnectionId +
                        (isArbiter ? ', isreplicaset: true, arbiterOnly: true' : '') +
                        '}'))
        internalConnection.enqueueReply(buildSuccessfulReply('{ok: 1, versionArray : [3, 0, 0]}'))
    }

    def enqueueSpeculativeAuthenticationResponsesForScramSha256() {
        def initialServerResponse = 'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
        def finalServerResponse = 'v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4='
        enqueueSpeculativeAuthenticationResponsesForScramSha(initialServerResponse, finalServerResponse)
    }

    def enqueueSpeculativeAuthenticationResponsesForScramSha1() {
        def initialServerResponse = 'r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096'
        def finalServerResponse = 'v=rmF9pqV8S7suAoZWja4dJRkFsKQ='
        enqueueSpeculativeAuthenticationResponsesForScramSha(initialServerResponse, finalServerResponse)
    }

    def enqueueSpeculativeAuthenticationResponsesForScramSha(final String initialServerResponse,
                                                             final String finalServerResponse) {
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1, maxWireVersion: 9, ' +
                        'ismaster: true, ' +
                        'speculativeAuthenticate: { conversationId: 1, done: false, ' +
                        "payload: BinData(0, '${encode64(initialServerResponse)}')}}"))
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1, maxWireVersion: 9, ' +
                        'conversationId: 1, done: true, ' +
                        "payload: BinData(0, '${encode64(finalServerResponse)}')}"))
        internalConnection.enqueueReply(buildSuccessfulReply('{ok: 1}'))
    }

    def enqueueSpeculativeAuthenticationResponsesForX509() {
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1, maxWireVersion: 9, ismaster: true, conversationId: 1, ' +
                        'speculativeAuthenticate: { dbname: \"$external\", ' +
                        'user: \"CN=client,OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US\" }}'))
        internalConnection.enqueueReply(buildSuccessfulReply('{ok: 1}'))
    }

    def enqueueSpeculativeAuthenticationResponsesForPlain() {
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1, maxWireVersion: 9, ismaster: true, conversationId: 1}'))
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1, done: true, conversationId: 1}'))
        internalConnection.enqueueReply(buildSuccessfulReply('{ok: 1}'))
    }

    def encode64(String string) {
        Base64.encode(string.getBytes(Charset.forName('UTF-8')))
    }

    def createIsMasterCommand(final String firstClientChallenge, final String mechanism,
                              final boolean hasSaslSupportedMechs) {
        String isMaster = '{ismaster: 1, helloOk: true, ' +
                (hasSaslSupportedMechs ? 'saslSupportedMechs: "database.user", ' : '') +
                (mechanism == 'MONGODB-X509' ?
                        'speculativeAuthenticate: { authenticate: 1, ' +
                                "mechanism: '${mechanism}', db: \"\$external\" } }" :
                        'speculativeAuthenticate: { saslStart: 1, ' +
                                "mechanism: '${mechanism}', payload: BinData(0, '${encode64(firstClientChallenge)}'), " +
                                'db: "admin", options: { skipEmptyExchange: true } } }')

        BsonDocument.parse(isMaster)
    }
}
