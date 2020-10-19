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

import com.mongodb.ClusterFixture
import com.mongodb.KerberosSubjectProvider
import com.mongodb.MongoCommandException
import com.mongodb.MongoCredential
import com.mongodb.MongoSecurityException
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import com.mongodb.connection.netty.NettyStreamFactory
import org.bson.BsonDocument
import org.bson.BsonString
import spock.lang.IgnoreIf
import spock.lang.Specification

import javax.security.auth.Subject
import javax.security.auth.login.LoginContext

import static com.mongodb.AuthenticationMechanism.GSSAPI
import static com.mongodb.ClusterFixture.getCredential
import static com.mongodb.ClusterFixture.getConnectionString
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.MongoCredential.JAVA_SUBJECT_PROVIDER_KEY
import static com.mongodb.MongoCredential.createGSSAPICredential
import static com.mongodb.internal.connection.CommandHelper.executeCommand
import static java.util.concurrent.TimeUnit.SECONDS

@IgnoreIf({ getCredential() == null || getCredential().getAuthenticationMechanism() != GSSAPI })
class GSSAPIAuthenticationSpecification extends Specification {

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
        def connection = createConnection(async, credentials)

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')), connection)

        then:
        true

        cleanup:
        connection?.close()

        where:
        [async, credentials] << [
                [true, false],
                [getMongoCredential(), getMongoCredential().withMechanismProperty(JAVA_SUBJECT_PROVIDER_KEY, new KerberosSubjectProvider())]
        ].combinations()
    }

    def 'should throw MongoSecurityException when authentication fails'() {
        given:
        def connection = createConnection(async, credentials)

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')), connection)

        then:
        thrown(MongoSecurityException)

        cleanup:
        connection?.close()

        where:
        [async, credentials] << [
                [true, false],
                [createGSSAPICredential('wrongUserName'),
                 createGSSAPICredential('wrongUserName')
                         .withMechanismProperty(JAVA_SUBJECT_PROVIDER_KEY, new KerberosSubjectProvider())]
        ].combinations()
    }

    def 'should authorize when successfully authenticated with Subject property'() {
        when:
        def loginContext = new LoginContext('com.sun.security.jgss.krb5.initiate')
        loginContext.login();
        def subject = loginContext.getSubject()

        then:
        subject != null
        subject.getPrincipals().size() == 1
        getMongoCredential().getUserName() == subject.getPrincipals().iterator().next().getName()

        when:
        def connection = createConnection(async, getMongoCredential(subject))
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')), connection)

        then:
        true

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    def 'should throw MongoSecurityException when authentication fails with Subject property'() {
        when:
        LoginContext context = new LoginContext('com.sun.security.jgss.krb5.initiate');
        context.login();

        Subject subject = context.getSubject();

        then:
        subject != null

        when:
        def connection = createConnection(async, getMongoCredential(createGSSAPICredential('wrongUserName'), subject))
        openConnection(connection, async)

        then:
        thrown(MongoSecurityException)

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    def 'should authorize when successfully authenticated with SaslClient properties'() {
        given:
        Map<String, Object> saslClientProperties = [:]

        when:
        def connection = createConnection(async, getMongoCredential(saslClientProperties))
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')), connection)

        then:
        true

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    private static MongoCredential getMongoCredential(final Map<String, Object> saslClientProperties) {
        getMongoCredential().withMechanismProperty(MongoCredential.JAVA_SASL_CLIENT_PROPERTIES_KEY, saslClientProperties);
    }

    private static MongoCredential getMongoCredential(final Subject subject) {
        getMongoCredential(getMongoCredential(), subject);
    }

    private static MongoCredential getMongoCredential(final MongoCredential mongoCredential, final Subject subject) {
        mongoCredential.withMechanismProperty(MongoCredential.JAVA_SUBJECT_KEY, subject)
    }

    private static MongoCredential getMongoCredential() {
        getCredential()
    }

    private static InternalStreamConnection createConnection(final boolean async, final MongoCredential credential) {
        new InternalStreamConnection(
                new ServerId(new ClusterId(), new ServerAddress(getConnectionString().getHosts().get(0))),
                async ? new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings())
                        : new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings()), [], null,
                new InternalStreamConnectionInitializer(createAuthenticator(credential), null, []))
    }

    private static Authenticator createAuthenticator(final MongoCredential credential) {
        credential == null ? null : new GSSAPIAuthenticator(new MongoCredentialWithCache(credential))
    }

    private static void openConnection(final InternalConnection connection, final boolean async) {
        if (async) {
            FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
            connection.openAsync(futureResultCallback)
            futureResultCallback.get(ClusterFixture.TIMEOUT, SECONDS);
        } else {
            connection.open()
        }
    }
}


