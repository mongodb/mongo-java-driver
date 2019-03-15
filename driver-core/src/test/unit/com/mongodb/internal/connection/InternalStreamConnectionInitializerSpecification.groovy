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

import com.mongodb.MongoCompressor
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.internal.connection.ClientMetadataHelperSpecification.createExpectedClientMetadataDocument
import static com.mongodb.internal.connection.MessageHelper.buildSuccessfulReply
import static com.mongodb.internal.connection.MessageHelper.decodeCommand

class InternalStreamConnectionInitializerSpecification extends Specification {

    def serverId = new ServerId(new ClusterId(), new ServerAddress())
    def internalConnection = new TestInternalConnection(serverId)

    def 'should create correct description'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(null, null, [])

        when:
        enqueueSuccessfulReplies(false, null)
        def description = initializer.initialize(internalConnection)

        then:
        description == getExpectedDescription(description.connectionId.localValue, null)
    }

    def 'should create correct description asynchronously'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(null, null, [])

        when:
        enqueueSuccessfulReplies(false, null)
        def futureCallback = new FutureResultCallback<ConnectionDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()

        then:
        description == getExpectedDescription(description.connectionId.localValue, null)
    }

    def 'should create correct description with server connection id'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(null, null, [])

        when:
        enqueueSuccessfulReplies(false, 123)
        def description = initializer.initialize(internalConnection)

        then:
        description == getExpectedDescription(description.connectionId.localValue, 123)
    }

    def 'should create correct description with server connection id from isMaster'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(null, null, [])

        when:
        enqueueSuccessfulRepliesWithConnectionIdIsIsMasterResponse(false, 123)
        def description = initializer.initialize(internalConnection)

        then:
        description == getExpectedDescription(description.connectionId.localValue, 123)
    }

    def 'should create correct description with server connection id asynchronously'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(null, null, [])

        when:
        enqueueSuccessfulReplies(false, 123)
        def futureCallback = new FutureResultCallback<ConnectionDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()

        then:
        description == getExpectedDescription(description.connectionId.localValue, 123)
    }

    def 'should create correct description with server connection id from isMaster asynchronously'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(null, null, [])

        when:
        enqueueSuccessfulRepliesWithConnectionIdIsIsMasterResponse(false, 123)
        def futureCallback = new FutureResultCallback<ConnectionDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()

        then:
        description == getExpectedDescription(description.connectionId.localValue, 123)
    }

    def 'should authenticate'() {
        given:
        def firstAuthenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer(firstAuthenticator, null, [])

        when:
        enqueueSuccessfulReplies(false, null)

        def description = initializer.initialize(internalConnection)

        then:
        description
        1 * firstAuthenticator.authenticate(internalConnection, _)
    }

    def 'should authenticate asynchronously'() {
        given:
        def authenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer(authenticator, null, [])

        when:
        enqueueSuccessfulReplies(false, null)

        def futureCallback = new FutureResultCallback<ConnectionDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()

        then:
        description
        1 * authenticator.authenticateAsync(internalConnection, _, _) >> { it[2].onResult(null, null) }
    }

    def 'should not authenticate if server is an arbiter'() {
        given:
        def authenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer(authenticator, null, [])

        when:
        enqueueSuccessfulReplies(true, null)

        def description = initializer.initialize(internalConnection)

        then:
        description
        0 * authenticator.authenticate(internalConnection, _)
    }

    def 'should not authenticate asynchronously if server is an arbiter asynchronously'() {
        given:
        def authenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer(authenticator, null, [])

        when:
        enqueueSuccessfulReplies(true, null)

        def futureCallback = new FutureResultCallback<ConnectionDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()

        then:
        description
        0 * authenticator.authenticateAsync(internalConnection, _, _)
    }

    def 'should add client metadata document to isMaster command'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(null, clientMetadataDocument, [])
        def expectedIsMasterCommandDocument = new BsonDocument('ismaster', new BsonInt32(1))
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
            initializer.initialize(internalConnection)
        }

        then:
        decodeCommand(internalConnection.getSent()[0]) == expectedIsMasterCommandDocument

        where:
        [clientMetadataDocument, async] << [[createExpectedClientMetadataDocument('appName'), null],
                                            [true, false]].combinations()
    }

    def 'should add compression to isMaster command'() {
        given:
        def initializer = new InternalStreamConnectionInitializer(null, null, compressors)
        def expectedIsMasterCommandDocument = new BsonDocument('ismaster', new BsonInt32(1))

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
            initializer.initialize(internalConnection)
        }

        then:
        decodeCommand(internalConnection.getSent()[0]) == expectedIsMasterCommandDocument

        where:
        [compressors, async] << [[[], [MongoCompressor.createZlibCompressor()]],
                                 [true, false]].combinations()
    }

    private ConnectionDescription getExpectedDescription(final Integer localValue, final Integer serverValue) {
        new ConnectionDescription(new ConnectionId(serverId, localValue, serverValue),
                new ServerVersion(3, 0), 3, ServerType.STANDALONE, 512, 16777216, 33554432, [])
    }

    def enqueueSuccessfulReplies(final boolean isArbiter, final Integer serverConnectionId) {
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1, ' +
                'maxWireVersion: 3' +
                (isArbiter ? ', isreplicaset: true, arbiterOnly: true' : '') +
                '}'))
        internalConnection.enqueueReply(buildSuccessfulReply('{ok: 1, versionArray : [3, 0, 0]}'))
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
}
