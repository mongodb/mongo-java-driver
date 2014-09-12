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
import com.mongodb.CursorFlag
import com.mongodb.MongoNamespace
import com.mongodb.MongoSecurityException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.DefaultClusterFactory
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import com.mongodb.protocol.message.CommandMessage
import com.mongodb.protocol.message.MessageSettings
import com.mongodb.selector.PrimaryServerSelector
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
import org.junit.experimental.categories.Category
import org.mongodb.Document

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSSLSettings
import static com.mongodb.MongoCredential.createMongoCRCredential
import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS

class UserOperationsSpecification extends OperationFunctionalSpecification {
    private User readOnlyUser
    private User readWriteUser

    def setup() {
        readOnlyUser = new User(createMongoCRCredential('jeff', databaseName, '123'.toCharArray()), true)
        readWriteUser = new User(createMongoCRCredential('jeff', databaseName, '123'.toCharArray()), false)
    }

    def 'an added user should be found'() {
        given:
        new CreateUserOperation(readOnlyUser).execute(getBinding())

        when:
        def found = new UserExistsOperation(databaseName, readOnlyUser.credential.userName).execute(getBinding())

        then:
        found

        cleanup:
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).execute(getBinding())
    }

    @Category(Async)
    def 'an added user should be found asynchronously'() {
        given:
        new CreateUserOperation(readOnlyUser).executeAsync(getAsyncBinding()).get()

        when:
        def found = new UserExistsOperation(databaseName, readOnlyUser.credential.userName)
                .executeAsync(getAsyncBinding()).get()

        then:
        found

        cleanup:
        new DropUserOperation(databaseName, readOnlyUser.credential.userName)
                .executeAsync(getAsyncBinding()).get()
    }

    def 'an added user should authenticate'() {
        given:
        new CreateUserOperation(readOnlyUser).execute(getBinding())
        def cluster = getCluster()

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        def connection = server.getConnection()
        sendMessage(connection)

        then:
        connection

        cleanup:
        connection?.release()
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).execute(getBinding())
        cluster?.close()
    }

    @Category(Async)
    def 'an added user should authenticate asynchronously'() {
        given:
        new CreateUserOperation(readOnlyUser).executeAsync(getAsyncBinding()).get()
        def cluster = getCluster()

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        def connection = server.getConnection()
        sendMessage(connection)

        then:
        connection

        cleanup:
        connection?.release()
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).executeAsync(getAsyncBinding()).get()
        cluster?.close()
    }

    def 'a removed user should not authenticate'() {
        given:
        new CreateUserOperation(readOnlyUser).execute(getBinding())
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).execute(getBinding())
        def cluster = getCluster()

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        sendMessage(server.getConnection())

        then:
        thrown(MongoSecurityException)

        cleanup:
        cluster?.close()
    }

    @Category(Async)
    def 'a removed user should not authenticate asynchronously'() {
        given:
        new CreateUserOperation(readOnlyUser).executeAsync(getAsyncBinding()).get()
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).executeAsync(getAsyncBinding()).get()
        def cluster = getCluster()

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        sendMessage(server.getConnection())

        then:
        thrown(MongoSecurityException)

        cleanup:
        cluster?.close()
    }

    def 'a replaced user should authenticate with its new password'() {
        given:
        new CreateUserOperation(readOnlyUser).execute(getBinding())
        def newUser = new User(createMongoCRCredential(readOnlyUser.credential.userName, readOnlyUser.credential.source,
                '234'.toCharArray()), true)
        new UpdateUserOperation(newUser).execute(getBinding())
        def cluster = getCluster(newUser)

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        def connection = server.getConnection()
        sendMessage(connection)

        then:
        connection

        cleanup:
        connection?.release()
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).execute(getBinding())
        cluster?.close()
    }

    @Category(Async)
    def 'a replaced user should authenticate with its new password asynchronously'() {
        given:
        new CreateUserOperation(readOnlyUser).executeAsync(getAsyncBinding()).get()
        def newUser = new User(createMongoCRCredential(readOnlyUser.credential.userName, readOnlyUser.credential.source,
                '234'.toCharArray()), true)
        new UpdateUserOperation(newUser).executeAsync(getAsyncBinding()).get()
        def cluster = getCluster(newUser)

        when:
        def server = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS)
        def connection = server.getConnection()
        sendMessage(connection)

        then:
        connection

        cleanup:
        connection?.release()
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).executeAsync(getAsyncBinding()).get()
        cluster?.close()
    }

    def 'a read write user should be able to write'() {
        given:
        new CreateUserOperation(readWriteUser).execute(getBinding())
        def cluster = getCluster()

        when:
        def result = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                                   asList(new InsertRequest(new BsonDocument())))
                .execute(getBinding())
        then:
        result.getCount() == 0

        cleanup:
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).execute(getBinding())
        cluster?.close()
    }

    @Category(Async)
    def 'a read write user should be able to write asynchronously'() {
        given:
        new CreateUserOperation(readWriteUser).executeAsync(getAsyncBinding()).get()
        def cluster = getCluster()

        when:
        def result = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                                   asList(new InsertRequest(new BsonDocument())))
                .executeAsync(getAsyncBinding()).get()
        then:
        result.getCount() == 0

        cleanup:
        new DropUserOperation(databaseName, readOnlyUser.credential.userName).executeAsync(getAsyncBinding()).get()
        cluster?.close()
    }

//    // This test is in UserOperationTest because the assertion is conditional on auth being enabled, and
//    // there's no way to do that in Spock
//    def 'a read only user should not be able to write'() {
//        given:
//        new CreateUserOperation(readOnlyUser).execute(getSession())
//        def cluster = getCluster()
//
//        when:
//        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
//                asList(new InsertRequest<Document>(new Document())),
//                new DocumentCodec())
//                .execute(getBinding())
//
//        then:
//        thrown(MongoWriteException)
//
//        cleanup:
//        new DropUserOperation(databaseName, readOnlyUser.credential.userName).execute(getBinding())
//        cluster?.close()
//    }

    def getCluster() {
        getCluster(readOnlyUser)
    }

    def getCluster(User user) {
        def streamFactory = new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings())
        new DefaultClusterFactory().create(ClusterSettings.builder().hosts(asList(getPrimary())).build(),
                ServerSettings.builder().build(),
                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                streamFactory, streamFactory, asList(user.credential), null, null, null)
    }

    def sendMessage(Connection connection) {
        def command = new CommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).getFullName(),
                                         new BsonDocument('ismaster', new BsonInt32(1)),
                                         EnumSet.noneOf(CursorFlag),
                                         MessageSettings.builder().build());
        OutputBuffer buffer = new BasicOutputBuffer();
        command.encode(buffer);
        connection.sendMessage(buffer.byteBuffers, command.getId())
    }
}
