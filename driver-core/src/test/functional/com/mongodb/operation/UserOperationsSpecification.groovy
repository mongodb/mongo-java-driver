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

package com.mongodb.operation

import category.Slow
import com.mongodb.MongoCredential
import com.mongodb.MongoNamespace
import com.mongodb.MongoSecurityException
import com.mongodb.MongoServerException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
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
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.selector.WritableServerSelector
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.Ignore
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getAsyncStreamFactory
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ClusterFixture.isAuthenticated
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.MongoCredential.createScramSha256Credential
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList


class UserOperationsSpecification extends OperationFunctionalSpecification {
    def credential = createCredential('\u53f0\u5317', databaseName, 'Ta\u0301ibe\u030Ci'.toCharArray())

    def 'an added user should be found'() {
        when:
        execute(new CreateUserOperation(credential, true), async)

        then:
        execute(new UserExistsOperation(databaseName, credential.userName), async)

        cleanup:
        execute(new DropUserOperation(databaseName, credential.userName), async)

        where:
        async << [true, false]
    }

    def 'an added user should authenticate'() {
        when:
        execute(new CreateUserOperation(credential, true), async)
        def cluster = getCluster()
        def server = cluster.selectServer(new WritableServerSelector())
        def connection = server.getConnection()

        then:
        !testConnection(connection).isEmpty()

        cleanup:
        connection?.release()
        execute(new DropUserOperation(databaseName, credential.userName), async)
        cluster?.close()

        where:
        async << [true, false]
    }

    // TODO: Waiting on final spec changes for SASL Prep
//    @IgnoreIf({ !serverVersionAtLeast(3, 7) })
    @Ignore
    def 'should correctly, prep username and be able to authenticate users with unicode in their name'() {
        when:
        def user = createScramSha256Credential('IX', databaseName, authCredential.getPassword())
        execute(new CreateUserOperation(user, true), async)
        def cluster = getCluster(authCredential)
        def server = cluster.selectServer(new WritableServerSelector())
        def connection = server.getConnection()

        then:
        !testConnection(connection).isEmpty()

        cleanup:
        connection?.release()
        execute(new DropUserOperation(databaseName, user.userName), async)
        cluster?.close()

        where:
        [async, authCredential] << [[true, false],
                [createCredential('I\u00ADX', databaseName, 'pass\u2010word' as char[]),
                 createScramSha256Credential('I\u00ADX', databaseName, 'pass\u2010word' as char[]),
                 createScramSha256Credential('\u2168', databaseName, 'pass\u2010word' as char[])]].combinations()
    }

    def 'should handle user not found'() {
        given:
        def credential = createCredential('user', databaseName, 'pencil' as char[])
        def cluster = getCluster(credential, ClusterSettings.builder().serverSelectionTimeout(1, TimeUnit.SECONDS))
        def server = cluster.selectServer(new WritableServerSelector())

        when:
        server.getConnection()

        then:
        thrown(MongoSecurityException)

        cleanup:
        cluster?.close()

        where:
        async << [true, false]
    }

    @Category(Slow)
    def 'a removed user should not authenticate'() {
        given:
        execute(new CreateUserOperation(credential, true), async)
        execute(new DropUserOperation(databaseName, credential.userName), async)
        def cluster = getCluster(ClusterSettings.builder().serverSelectionTimeout(1, TimeUnit.SECONDS))
        def server = cluster.selectServer(new WritableServerSelector())

        when:
        server.getConnection()

        then:
        thrown(MongoSecurityException)

        cleanup:
        cluster?.close()

        where:
        async << [true, false]
    }

    def 'a replaced user should authenticate with its new password'() {
        given:
        execute(new CreateUserOperation(credential, true), async)
        def newCredentials = createCredential(credential.userName, credential.source, '234'.toCharArray())
                execute(new UpdateUserOperation(newCredentials, true), async)
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
        execute(new DropUserOperation(databaseName, credential.userName), async)

        where:
        async << [true, false]
    }

    def 'a read write user should be able to write'() {
        when:
        execute(new CreateUserOperation(credential, false), async)
        def cluster = async ? getAsyncCluster() : getCluster()
        def binding = async ? getAsyncBinding(cluster) : getBinding(cluster)

        then:
        execute(new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, asList(new InsertRequest(new BsonDocument()))),
                binding).getCount() == 0

        cleanup:
        execute(new DropUserOperation(databaseName, credential.userName), async)
        cluster?.close()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isAuthenticated() || isSharded()  })
    def 'a read only user should not be able to write'() {
        given:
        execute(new CreateUserOperation(credential, true), async)
        def cluster = getCluster(credential)

        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false,
                asList(new InsertRequest(new BsonDocument()))).execute(getBinding(cluster))

        then:
        thrown(MongoServerException)

        cleanup:
        execute(new DropUserOperation(databaseName, credential.userName), async)
        cluster?.close()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isAuthenticated() })
    def 'a read write admin user should be able to write to a different database'() {
        given:
        def rwCredential = createCredential('jeff-rw-admin', 'admin', '123'.toCharArray())
        execute(new CreateUserOperation(rwCredential, false), async)
        def cluster = async ? getAsyncCluster(rwCredential) : getCluster(rwCredential)
        def binding = async ? getAsyncBinding(cluster) : getBinding(cluster)

        when:
        execute(new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, asList(new InsertRequest(new BsonDocument()))), binding)

        then:
        execute(new CountOperation(getNamespace()), binding) == 1L

        cleanup:
        execute(new DropUserOperation('admin', rwCredential.userName), async)
        cluster?.close()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isAuthenticated() || isSharded() })
    def 'a read only admin user should not be able to write to a different database'() {
        given:
        def roCredential = createCredential('jeff-ro-admin', 'admin', '123'.toCharArray())
        execute(new CreateUserOperation(roCredential, true), async)
        def cluster = async ? getAsyncCluster(roCredential) : getCluster(roCredential)
        def binding = async ? getAsyncBinding(cluster) : getBinding(cluster)

        when:
        execute(new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, asList(new InsertRequest(new BsonDocument()))), binding)

        then:
        thrown(MongoServerException)

        cleanup:
        execute(new DropUserOperation('admin', roCredential.userName), async)
        cluster?.close()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isAuthenticated() })
    def 'a read only admin user should be able to read from a different database'() {
        when:
        def roCredential = createCredential('jeff-ro-admin', 'admin', '123'.toCharArray())
        execute(new CreateUserOperation(roCredential, true), async)
        def cluster = async ? getAsyncCluster(roCredential) : getCluster(roCredential)
        def binding = async ? getAsyncBinding(cluster) : getBinding(cluster)

        then:
        execute(new CountOperation(getNamespace()), binding) == 0L

        cleanup:
        execute(new DropUserOperation('admin', roCredential.userName), async)
        cluster?.close()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error when creating a user'() {
        given:
        def operation = new CreateUserOperation(credential, false, new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.code == 100

        when:
        operation = new UpdateUserOperation(credential, true, new WriteConcern(5))
        execute(operation, async)

        then:
        ex = thrown(MongoWriteConcernException)
        ex.code == 100

        cleanup:
        execute(new DropUserOperation(databaseName, credential.userName), async)

        where:
        async << [true, false]
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
        _ * connection.getDescription() >> helper.twoSixConnectionDescription
        1 * connection.command(helper.dbName, _, _, readPreference, _, _) >> helper.cursorResult

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
        _ * connection.getDescription() >> helper.twoSixConnectionDescription
        1 * connection.commandAsync(helper.dbName, _, _, readPreference, _, _, _) >> {
            it[6].onResult(helper.cursorResult, null) }

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def helper = [
            dbName: 'db',
            namespace: new MongoNamespace('db', 'coll'),
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
        new DefaultClusterFactory().createCluster(builder.hosts(asList(getPrimary())).build(),
                                           ServerSettings.builder().build(),
                                           ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                                           streamFactory, streamFactory, asList(credential), null, null, null, [])
    }

    def testConnection(Connection connection) {
        connection.command('admin', new BsonDocument('ismaster', new BsonInt32(1)), NO_OP_FIELD_NAME_VALIDATOR,
                ReadPreference.primaryPreferred(), new BsonDocumentCodec(), NoOpSessionContext.INSTANCE)
    }
}
