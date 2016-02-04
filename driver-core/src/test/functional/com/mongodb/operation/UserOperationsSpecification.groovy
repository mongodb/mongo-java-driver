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
import category.Slow
import com.mongodb.MongoCredential
import com.mongodb.MongoNamespace
import com.mongodb.MongoServerException
import com.mongodb.MongoTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.bulk.InsertRequest
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.DefaultClusterFactory
import com.mongodb.connection.QueryResult
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.ServerVersion
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import com.mongodb.connection.StreamFactory
import com.mongodb.internal.validator.NoOpFieldNameValidator
import com.mongodb.selector.WritableServerSelector
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getAsyncStreamFactory
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ClusterFixture.isAuthenticated
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList

class UserOperationsSpecification extends OperationFunctionalSpecification {
    def credential = createCredential('\u53f0\u5317', databaseName, 'Ta\u0301ibe\u030Ci'.toCharArray())

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
        executeAsync(new CreateUserOperation(credential, true))

        when:
        def found = executeAsync(new UserExistsOperation(databaseName, credential.userName))

        then:
        found

        cleanup:
        executeAsync(new DropUserOperation(databaseName, credential.userName))
    }

    def 'an added user should authenticate'() {
        given:
        new CreateUserOperation(credential, true).execute(getBinding())
        def cluster = getCluster()

        when:
        def server = cluster.selectServer(new WritableServerSelector())
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
        executeAsync(new CreateUserOperation(credential, true))
        def cluster = getCluster()

        when:
        def server = cluster.selectServer(new WritableServerSelector())
        def connection = server.getConnection()
        testConnection(connection)

        then:
        connection

        cleanup:
        connection?.release()
        cluster?.close()
        executeAsync(new DropUserOperation(databaseName, credential.userName))
    }

    @Category(Slow)
    def 'a removed user should not authenticate'() {
        given:
        new CreateUserOperation(credential, true).execute(getBinding())
        new DropUserOperation(databaseName, credential.userName).execute(getBinding())
        def cluster = getCluster(ClusterSettings.builder().serverSelectionTimeout(1, TimeUnit.SECONDS))

        when:
        cluster.selectServer(new WritableServerSelector())

        then:
        thrown(MongoTimeoutException)

        cleanup:
        cluster?.close()
    }

    @Category([Async, Slow])
    def 'a removed user should not authenticate asynchronously'() {
        given:
        executeAsync(new CreateUserOperation(credential, true))
        executeAsync(new DropUserOperation(databaseName, credential.userName))
        def cluster = getCluster(ClusterSettings.builder().serverSelectionTimeout(1, TimeUnit.SECONDS))

        when:
        cluster.selectServer(new WritableServerSelector())

        then:
        thrown(MongoTimeoutException)

        cleanup:
        cluster?.close()
    }

    def 'a replaced user should authenticate with its new password'() {
        given:
        new CreateUserOperation(credential, true).execute(getBinding())
        def newCredentials = createCredential(credential.userName, credential.source, '234'.toCharArray())
        new UpdateUserOperation(newCredentials, true).execute(getBinding())
        def cluster = getCluster(newCredentials)

        when:
        def server = cluster.selectServer(new WritableServerSelector())
        def connection = server.getConnection()
        testConnection(connection)

        then:
        connection

        cleanup:
        connection?.release()
        cluster?.close()
        new DropUserOperation(databaseName, credential.userName).execute(getBinding())
    }

    @Category(Async)
    def 'a replaced user should authenticate with its new password asynchronously'() {
        given:
        executeAsync(new CreateUserOperation(credential, true))
        def newCredentials = createCredential(credential.userName, credential.source, '234'.toCharArray())
        executeAsync(new UpdateUserOperation(newCredentials, true))
        def cluster = getAsyncCluster(newCredentials)

        when:
        def server = cluster.selectServer(new WritableServerSelector())
        def connection = server.getConnection()
        testConnection(connection)

        then:
        connection

        cleanup:
        connection?.release()
        cluster?.close()
        executeAsync(new DropUserOperation(databaseName, credential.userName))
    }

    def 'a read write user should be able to write'() {
        given:
        new CreateUserOperation(credential, false).execute(getBinding())
        def cluster = getCluster()

        when:
        def result = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))
                .execute(getBinding(cluster))
        then:
        result.getCount() == 0

        cleanup:
        new DropUserOperation(databaseName, credential.userName).execute(getBinding(cluster))
        cluster?.close()
    }

    @Category(Async)
    def 'a read write user should be able to write asynchronously'() {
        given:
        executeAsync(new CreateUserOperation(credential, false))
        def cluster = getAsyncCluster()

        when:
        def result = executeAsync(new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument()))),
                                  getAsyncBinding(cluster))
        then:
        result.getCount() == 0

        cleanup:
        executeAsync(new DropUserOperation(databaseName, credential.userName))
        cluster?.close()
    }

    @IgnoreIf({ !isAuthenticated() || isSharded()  })
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
        def rwCredential = createCredential('jeff-rw-admin', 'admin', '123'.toCharArray());
        new CreateUserOperation(rwCredential, false).execute(getBinding())
        def cluster = getCluster(rwCredential)

        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED,
                            asList(new InsertRequest(new BsonDocument()))).execute(getBinding(cluster))

        then:
        new CountOperation(getNamespace()).execute(getBinding(cluster)) == 1L

        cleanup:
        new DropUserOperation('admin', rwCredential.userName).execute(getBinding())
        cluster?.close()
    }

    @IgnoreIf({ !isAuthenticated() || isSharded() })
    def 'a read only admin user should not be able to write to a different database'() {
        given:
        def roCredential = createCredential('jeff-ro-admin', 'admin', '123'.toCharArray());
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
        def roCredential = createCredential('jeff-ro-admin', 'admin', '123'.toCharArray());
        new CreateUserOperation(roCredential, true).execute(getBinding())
        def cluster = getCluster(roCredential)

        when:
        def count = new CountOperation(getNamespace()).execute(getBinding())

        then:
        count == 0L

        cleanup:
        new DropUserOperation('admin', roCredential.userName).execute(getBinding())
        cluster?.close()
    }


    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(Connection)
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
        }
        def operation = new UserExistsOperation(helper.dbName, 'user')

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.twoFourConnectionDescription
        1 * connection.query(_, _, _, _, _, _, readPreference.isSlaveOk(), _, _, _, _, _, _) >>  helper.queryResult
        1 * connection.release()

        when: '2.6.0'
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.twoSixConnectionDescription
        1 * connection.command(helper.dbName, _, readPreference.isSlaveOk(), _, _) >> helper.cursorResult

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadPreference() >> readPreference
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new UserExistsOperation(helper.dbName, 'user')

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.twoFourConnectionDescription
        1 * connection.queryAsync(_, _, _, _, _, _, readPreference.isSlaveOk(), _, _, _, _, _, _, _) >> {
            it[13].onResult(helper.queryResult, null) }

        when: '2.6.0'
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.twoSixConnectionDescription
        1 * connection.commandAsync(helper.dbName, _, readPreference.isSlaveOk(), _, _, _) >> {
            it[5].onResult(helper.cursorResult, null) }

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def helper = [
            dbName: 'db',
            namespace: new MongoNamespace('db', 'coll'),
            twoFourConnectionDescription: Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([2, 4, 0])
            },
            twoSixConnectionDescription : Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([2, 6, 0])
            },
            queryResult: Stub(QueryResult),
            cursorResult: BsonDocument.parse('{ok: 1.0, users: []}')
    ]

    def getCluster() {
        getCluster(credential)
    }

    def getCluster(ClusterSettings.Builder builder) {
        getCluster(credential, builder)
    }

    def getCluster(MongoCredential credential) {
        getCluster(credential, ClusterSettings.builder())
    }

    def getCluster(MongoCredential credential, ClusterSettings.Builder builder) {
        getCluster(credential, new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings()), builder)
    }

    def getAsyncCluster() {
        getAsyncCluster(credential)
    }

    def getAsyncCluster(MongoCredential credential) {
        getCluster(credential, getAsyncStreamFactory(), ClusterSettings.builder())
    }

    def getCluster(MongoCredential credential, StreamFactory streamFactory, ClusterSettings.Builder builder) {
        new DefaultClusterFactory().create(builder.hosts(asList(getPrimary())).build(),
                                           ServerSettings.builder().build(),
                                           ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                                           streamFactory, streamFactory, asList(credential), null, null, null)
    }

    def testConnection(Connection connection) {
        connection.command('admin', new BsonDocument('ismaster', new BsonInt32(1)), false, new NoOpFieldNameValidator(),
                           new BsonDocumentCodec())
    }
}
