package com.mongodb.connection

import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.connection.ClientMetadataHelperSpecification.createExpectedClientMetadataDocument
import static com.mongodb.connection.MessageHelper.buildSuccessfulReply
import static com.mongodb.connection.MessageHelper.decodeCommand

class InternalStreamConnectionInitializerSpecification extends Specification {

    def serverId = new ServerId(new ClusterId(), new ServerAddress())
    def internalConnection = new TestInternalConnection(serverId)

    def 'should create correct description'() {
        given:
        def initializer = new InternalStreamConnectionInitializer([], null)

        when:
        enqueueSuccessfulReplies(false, null)
        def description = initializer.initialize(internalConnection)

        then:
        description == getExpectedDescription(description.connectionId.localValue, null)
    }

    def 'should create correct description asynchronously'() {
        given:
        def initializer = new InternalStreamConnectionInitializer([], null)

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
        def initializer = new InternalStreamConnectionInitializer([], null)

        when:
        enqueueSuccessfulReplies(false, 123)
        def description = initializer.initialize(internalConnection)

        then:
        description == getExpectedDescription(description.connectionId.localValue, 123)
    }

    def 'should create correct description with server connection id asynchronously'() {
        given:
        def initializer = new InternalStreamConnectionInitializer([], null)

        when:
        enqueueSuccessfulReplies(false, 123)
        def futureCallback = new FutureResultCallback<ConnectionDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()

        then:
        description == getExpectedDescription(description.connectionId.localValue, 123)
    }

    def 'should authenticate multiple credentials'() {
        given:
        def firstAuthenticator = Mock(Authenticator)
        def secondAuthenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer([firstAuthenticator, secondAuthenticator], null)

        when:
        enqueueSuccessfulReplies(false, null)

        def description = initializer.initialize(internalConnection)

        then:
        description
        1 * firstAuthenticator.authenticate(internalConnection, _)
        1 * secondAuthenticator.authenticate(internalConnection, _)
    }

    def 'should authenticate multiple credentials asynchronously'() {
        given:
        def firstAuthenticator = Mock(Authenticator)
        def secondAuthenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer([firstAuthenticator, secondAuthenticator], null)

        when:
        enqueueSuccessfulReplies(false, null)

        def futureCallback = new FutureResultCallback<ConnectionDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()

        then:
        description
        1 * firstAuthenticator.authenticateAsync(internalConnection, _, _) >> { it[2].onResult(null, null) }
        1 * secondAuthenticator.authenticateAsync(internalConnection, _, _) >> { it[2].onResult(null, null) }
    }

    def 'should not authenticate if server is an arbiter'() {
        given:
        def firstAuthenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer([firstAuthenticator], null)

        when:
        enqueueSuccessfulReplies(true, null)

        def description = initializer.initialize(internalConnection)

        then:
        description
        0 * firstAuthenticator.authenticate(internalConnection, _)
    }

    def 'should not authenticate asynchronously if server is an arbiter asynchronously'() {
        given:
        def firstAuthenticator = Mock(Authenticator)
        def initializer = new InternalStreamConnectionInitializer([firstAuthenticator], null)

        when:
        enqueueSuccessfulReplies(true, null)

        def futureCallback = new FutureResultCallback<ConnectionDescription>()
        initializer.initializeAsync(internalConnection, futureCallback)
        def description = futureCallback.get()

        then:
        description
        0 * firstAuthenticator.authenticateAsync(internalConnection, _, _)
    }

    def 'should add client metadata document to isMaster command'() {
        given:
        def initializer = new InternalStreamConnectionInitializer([], clientMetadataDocument)
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

    private ConnectionDescription getExpectedDescription(final Integer localValue, final Integer serverValue) {
        new ConnectionDescription(new ConnectionId(serverId, localValue, serverValue),
                                  new ServerVersion(3, 0), ServerType.STANDALONE, 512, 16777216, 33554432)
    }

    def enqueueSuccessfulReplies(final boolean isArbiter, final Integer serverConnectionId) {
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1' +
                (isArbiter ? ', isreplicaset: true, arbiterOnly: true' : '') +
                '}'))
        internalConnection.enqueueReply(buildSuccessfulReply('{ok: 1, versionArray : [3, 0, 0]}'))
        internalConnection.enqueueReply(buildSuccessfulReply(
                '{ok: 1 ' +
                (serverConnectionId == null ? '' : ', connectionId: ' + serverConnectionId) +
                '}'))
    }
}