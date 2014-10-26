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

package com.mongodb.operation

import category.Async
import com.mongodb.MongoCredential
import com.mongodb.MongoServerException
import com.mongodb.MongoTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.DefaultClusterFactory
import com.mongodb.connection.NoOpFieldNameValidator
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import com.mongodb.selector.PrimaryServerSelector
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSSLSettings
import static com.mongodb.ClusterFixture.isAuthenticated
import static com.mongodb.MongoCredential.createMongoCRCredential
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS

class UserOperationsSpecification extends OperationFunctionalSpecification {
    def credential = createMongoCRCredential('jeff', databaseName, '123'.toCharArray())

    def 'an added user should be found'() {
        given:
        new CreateUserOperation(credential, true).execute(getBinding())

        when:
        def found = new UserExistsOperation(databaseName, credential.userName).execute(getBinding())

        then:
        found

        cleanup:
        new DropUserOperation(databaseName, credential.userName).execute(getBinding())
    }

    @Category(Async)
    def 'an added user should be found asynchronously'() {
        given:
        new CreateUserOperation(credential, true).executeAsync(getAsyncBinding()).get()

        when:
        def found = new UserExistsOperation(databaseName, credential.userName)
                .executeAsync(getAsyncBinding()).get()

        then:
        found

        cleanup:
        new DropUserOperation(databaseName, credential.userName)
                .executeAsync(getAsyncBinding()).get()
    }

    def 'an added user should authenticate'() {
        given:
        new CreateUserOperation(credential, true).execute(getBinding())
        def cluster = getCluster()

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        def connection = server.getConnection()
        testConnection(connection)

        then:
        connection

        cleanup:
        connection?.release()
        new DropUserOperation(databaseName, credential.userName).execute(getBinding())
        cluster?.close()
    }

    @Category(Async)
    def 'an added user should authenticate asynchronously'() {
        given:
        new CreateUserOperation(credential, true).executeAsync(getAsyncBinding()).get()
        def cluster = getCluster()

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        def connection = server.getConnection()
        testConnection(connection)

        then:
        connection

        cleanup:
        connection?.release()
        new DropUserOperation(databaseName, credential.userName).executeAsync(getAsyncBinding()).get()
        cluster?.close()
    }

    def 'a removed user should not authenticate'() {
        given:
        new CreateUserOperation(credential, true).execute(getBinding())
        new DropUserOperation(databaseName, credential.userName).execute(getBinding())
        def cluster = getCluster()

        when:
        cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)

        then:
        thrown(MongoTimeoutException)

        cleanup:
        cluster?.close()
    }

    @Category(Async)
    def 'a removed user should not authenticate asynchronously'() {
        given:
        new CreateUserOperation(credential, true).executeAsync(getAsyncBinding()).get()
        new DropUserOperation(databaseName, credential.userName).executeAsync(getAsyncBinding()).get()
        def cluster = getCluster()

        when:
        cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)

        then:
        thrown(MongoTimeoutException)

        cleanup:
        cluster?.close()
    }

    def 'a replaced user should authenticate with its new password'() {
        given:
        new CreateUserOperation(credential, true).execute(getBinding())
        def newCredentials = createMongoCRCredential(credential.userName, credential.source, '234'.toCharArray())
        new UpdateUserOperation(newCredentials, true).execute(getBinding())
        def cluster = getCluster(newCredentials)

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        def connection = server.getConnection()
        testConnection(connection)

        then:
        connection

        cleanup:
        connection?.release()
        new DropUserOperation(databaseName, credential.userName).execute(getBinding())
        cluster?.close()
    }

    @Category(Async)
    def 'a replaced user should authenticate with its new password asynchronously'() {
        given:
        new CreateUserOperation(credential, true).executeAsync(getAsyncBinding()).get()
        def newCredentials = createMongoCRCredential(credential.userName, credential.source, '234'.toCharArray())
        new UpdateUserOperation(newCredentials, true).executeAsync(getAsyncBinding()).get()
        def cluster = getCluster(newCredentials)

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        def connection = server.getConnection()
        testConnection(connection)

        then:
        connection

        cleanup:
        connection?.release()
        new DropUserOperation(databaseName, credential.userName).executeAsync(getAsyncBinding()).get()
        cluster?.close()
    }

    def 'a read write user should be able to write'() {
        given:
        new CreateUserOperation(credential, false).execute(getBinding())
        def cluster = getCluster()

        when:
        def result = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))
                .execute(getBinding())
        then:
        result.getCount() == 0

        cleanup:
        new DropUserOperation(databaseName, credential.userName).execute(getBinding())
        cluster?.close()
    }

    @Category(Async)
    def 'a read write user should be able to write asynchronously'() {
        given:
        new CreateUserOperation(credential, false).executeAsync(getAsyncBinding()).get()
        def cluster = getCluster()

        when:
        def result = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))
                .executeAsync(getAsyncBinding()).get()
        then:
        result.getCount() == 0

        cleanup:
        new DropUserOperation(databaseName, credential.userName).executeAsync(getAsyncBinding()).get()
        cluster?.close()
    }

    @IgnoreIf({ !isAuthenticated() })
    def 'a read only user should not be able to write'() {
        given:
        new CreateUserOperation(credential, true).execute(getBinding())
        def cluster = getCluster(credential)

        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED,
                            asList(new InsertRequest(new BsonDocument()))).execute(getBinding(cluster))

        then:
        thrown(MongoServerException)

        cleanup:
        new DropUserOperation(databaseName, credential.userName).execute(getBinding())
        cluster?.close()
    }

    @IgnoreIf({ !isAuthenticated() })
    def 'a read write admin user should be able to write to a different database'() {
        given:
        def rwCredential = createMongoCRCredential('jeff-rw-admin', 'admin', '123'.toCharArray());
        new CreateUserOperation(rwCredential, false).execute(getBinding())
        def cluster = getCluster(rwCredential)

        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED,
                            asList(new InsertRequest(new BsonDocument()))).execute(getBinding(cluster))

        then:
        new CountOperation(getNamespace()).execute(getBinding(cluster)) == 1

        cleanup:
        new DropUserOperation('admin', rwCredential.userName).execute(getBinding())
        cluster?.close()
    }

    @IgnoreIf({ !isAuthenticated() })
    def 'a read only admin user should not be able to write to a different database'() {
        given:
        def roCredential = createMongoCRCredential('jeff-ro-admin', 'admin', '123'.toCharArray());
        new CreateUserOperation(roCredential, true).execute(getBinding())
        def cluster = getCluster(roCredential)

        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED,
                            asList(new InsertRequest(new BsonDocument()))).execute(getBinding(cluster))

        then:
        thrown(MongoServerException)

        cleanup:
        new DropUserOperation('admin', roCredential.userName).execute(getBinding())
        cluster?.close()
    }

    @IgnoreIf({ !isAuthenticated() })
    def 'a read only admin user should be able to read from a different database'() {
        given:
        def roCredential = createMongoCRCredential('jeff-ro-admin', 'admin', '123'.toCharArray());
        new CreateUserOperation(roCredential, true).execute(getBinding())
        def cluster = getCluster(roCredential)

        when:
        def count = new CountOperation(getNamespace()).execute(getBinding())

        then:
        count == 0

        cleanup:
        new DropUserOperation('admin', roCredential.userName).execute(getBinding())
        cluster?.close()
    }

    def getCluster() {
        getCluster(credential)
    }

    def getCluster(MongoCredential credential) {
        def streamFactory = new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings())
        new DefaultClusterFactory().create(ClusterSettings.builder().hosts(asList(getPrimary())).build(),
                ServerSettings.builder().build(),
                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                streamFactory, streamFactory, asList(credential), null, null, null)
    }

    def testConnection(Connection connection) {
        connection.command('admin', new BsonDocument('ismaster', new BsonInt32(1)), false, new NoOpFieldNameValidator(),
                           new BsonDocumentCodec())
    }
}
