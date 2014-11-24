package com.mongodb.connection

import category.Async
import com.mongodb.CommandFailureException
import com.mongodb.MongoException
import com.mongodb.ServerAddress
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.junit.experimental.categories.Category
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.getCredentialList
import static com.mongodb.ClusterFixture.getSSLSettings
import static com.mongodb.connection.CommandHelper.executeCommandAsync

class CommandHelperSpecification extends Specification {
    InternalConnection connection

    def setup() {
        connection = new InternalStreamConnection(new ServerId(new ClusterId(), new ServerAddress()),
                                                  new AsynchronousSocketChannelStreamFactory(SocketSettings.builder().build(),
                                                                                             getSSLSettings()),
                                                  new InternalStreamConnectionInitializer(getCredentialList()),
                                                  new NoOpConnectionListener())
        connection.open()
    }
    def cleanup() {
        connection?.close()
    }

    @Category(Async)
    def 'should execute command asynchronously'() {
        when:
        BsonDocument receivedDocument
        MongoException receivedException
        def latch1 = new CountDownLatch(1)
        executeCommandAsync('admin', new BsonDocument('ismaster', new BsonInt32(1)), connection)
                { document, exception -> receivedDocument = document; receivedException = exception; latch1.countDown() }
        latch1.await()

        then:
        receivedDocument
        receivedDocument.containsKey('ok')
        !receivedException

        when:
        def latch2 = new CountDownLatch(1)
        executeCommandAsync('admin', new BsonDocument('non-existent-command', new BsonInt32(1)), connection)
                { document, exception -> receivedDocument = document; receivedException = exception; latch2.countDown() }
        latch2.await()

        then:
        !receivedDocument
        receivedException instanceof CommandFailureException
    }

}