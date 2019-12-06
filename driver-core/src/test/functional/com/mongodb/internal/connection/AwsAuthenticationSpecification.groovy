package com.mongodb.internal.connection

import com.mongodb.ClusterFixture
import com.mongodb.MongoCommandException
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.AsynchronousSocketChannelStreamFactory
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import org.bson.BsonDocument
import org.bson.BsonString
import spock.lang.IgnoreIf
import spock.lang.Specification

import static com.mongodb.AuthenticationMechanism.MONGODB_AWS
import static com.mongodb.ClusterFixture.getConnectionString
import static com.mongodb.ClusterFixture.getCredential
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.internal.connection.CommandHelper.executeCommand
import static java.util.concurrent.TimeUnit.SECONDS

@IgnoreIf({ getCredential() == null || getCredential().getAuthenticationMechanism() != MONGODB_AWS })
class AwsAuthenticationSpecification extends Specification {

    def 'should not authorize when not authenticated'() {
        given:
        def connection = createConnection(async, null)

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')), connection)

        then:
        thrown(MongoCommandException)

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    def 'should authorize when successfully authenticated'() {
        given:
        def connection = createConnection(async, getCredential())

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')), connection)

        then:
        true

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    private static InternalStreamConnection createConnection(final boolean async, final MongoCredential credential) {
        new InternalStreamConnection(
                new ServerId(new ClusterId(), new ServerAddress(getConnectionString().getHosts().get(0))),
                async ? new AsynchronousSocketChannelStreamFactory(SocketSettings.builder().build(), getSslSettings())
                        : new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings()), [], null,
                new InternalStreamConnectionInitializer(createAuthenticator(credential), null, []))
    }

    private static Authenticator createAuthenticator(final MongoCredential credential) {
        credential == null ? null : new AwsAuthenticator(new MongoCredentialWithCache(credential))
    }

    private static void openConnection(final InternalConnection connection, final boolean async) {
        if (async) {
            FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>()
            connection.openAsync(futureResultCallback)
            futureResultCallback.get(ClusterFixture.TIMEOUT, SECONDS)
        } else {
            connection.open()
        }
    }
}
