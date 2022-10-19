package com.mongodb.internal.connection

import com.mongodb.AwsCredential
import com.mongodb.ClusterFixture
import com.mongodb.MongoCommandException
import com.mongodb.MongoCredential
import com.mongodb.MongoSecurityException
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.AsynchronousSocketChannelStreamFactory
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import com.mongodb.internal.authentication.AwsCredentialHelper
import org.bson.BsonDocument
import org.bson.BsonString
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.util.function.Supplier

import static com.mongodb.AuthenticationMechanism.MONGODB_AWS
import static com.mongodb.ClusterFixture.getClusterConnectionMode
import static com.mongodb.ClusterFixture.getConnectionString
import static com.mongodb.ClusterFixture.getCredential
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.internal.connection.CommandHelper.executeCommand
import static java.util.concurrent.TimeUnit.SECONDS

@IgnoreIf({ getCredential() == null || getCredential().getAuthenticationMechanism() != MONGODB_AWS })
class AwsAuthenticationSpecification extends Specification {

    static {
        def providerProperty = System.getProperty('org.mongodb.test.aws.credential.provider', 'awsSdkV2')

        if (providerProperty == 'builtIn') {
            AwsCredentialHelper.requireBuiltInProvider()
        } else if (providerProperty == 'awsSdkV1') {
            AwsCredentialHelper.requireAwsSdkV1Provider()
        } else if (providerProperty == 'awsSdkV2') {
            AwsCredentialHelper.requireAwsSdkV2Provider()
        } else {
            throw new IllegalArgumentException("Unrecognized AWS credential provider: $providerProperty")
        }
    }

    def 'should not authorize when not authenticated'() {
        given:
        def connection = createConnection(async, null)

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')),
                getClusterConnectionMode(), null, connection)

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
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')),
                getClusterConnectionMode(), null, connection)

        then:
        true

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    @IgnoreIf({ System.getenv('AWS_SESSION_TOKEN') == null || System.getenv('AWS_SESSION_TOKEN') == '' })
    def 'should authorize when successfully authenticated using provider'() {
        given:
        def connection = createConnection(async,
                getCredential().withMechanismProperty(MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY,
                    new Supplier<AwsCredential>() {
                        @Override
                        AwsCredential get() {
                            new AwsCredential(
                                    System.getenv('AWS_ACCESS_KEY_ID'),
                                    System.getenv('AWS_SECRET_ACCESS_KEY'),
                                    System.getenv('AWS_SESSION_TOKEN'))
                        }
                    }))

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')),
                getClusterConnectionMode(), null, connection)

        then:
        true

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    // This test is just proving that the credential provider is not being totally ignored
    @IgnoreIf({ System.getenv('AWS_SESSION_TOKEN') == null || System.getenv('AWS_SESSION_TOKEN') == '' })
    def 'should not authenticate when provider gives invalid session token'() {
        given:
        def connection = createConnection(async,
                getCredential().withMechanismProperty(MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY,
                        new Supplier<AwsCredential>() {
                            @Override
                            AwsCredential get() {
                                new AwsCredential(
                                        System.getenv('AWS_ACCESS_KEY_ID'),
                                        System.getenv('AWS_SECRET_ACCESS_KEY'),
                                        'fake-session-token')
                            }
                        }))

        when:
        openConnection(connection, async)

        then:
        thrown(MongoSecurityException)

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    private static InternalStreamConnection createConnection(final boolean async, final MongoCredential credential) {
        new InternalStreamConnection(SINGLE,
                new ServerId(new ClusterId(), new ServerAddress(getConnectionString().getHosts().get(0))),
                new TestConnectionGenerationSupplier(),
                async ? new AsynchronousSocketChannelStreamFactory(SocketSettings.builder().build(), getSslSettings())
                        : new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings()), [], null,
                new InternalStreamConnectionInitializer(SINGLE, createAuthenticator(credential), null, [], null))
    }

    private static Authenticator createAuthenticator(final MongoCredential credential) {
        credential == null ? null : new AwsAuthenticator(new MongoCredentialWithCache(credential), SINGLE, null)
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
