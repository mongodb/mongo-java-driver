/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation

import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.codecs.DocumentCodec
import org.mongodb.connection.ClusterSettings
import org.mongodb.connection.ConnectionPoolSettings
import org.mongodb.connection.DefaultClusterFactory
import org.mongodb.connection.MongoSecurityException
import org.mongodb.connection.ServerSettings
import org.mongodb.connection.SocketSettings
import org.mongodb.connection.SocketStreamFactory
import org.mongodb.session.ClusterSession
import org.mongodb.session.PrimaryServerSelector

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

import static java.util.Arrays.asList
import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getExecutor
import static org.mongodb.Fixture.getPrimary
import static org.mongodb.Fixture.getSSLSettings
import static org.mongodb.Fixture.getSession
import static org.mongodb.MongoCredential.createMongoCRCredential
import static org.mongodb.WriteConcern.ACKNOWLEDGED

class UserOperationsSpecification extends FunctionalSpecification {
    private ScheduledExecutorService scheduledExecutorService
    private User readOnlyUser
    private User readWriteUser

    def setup() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
        readOnlyUser = new User(createMongoCRCredential('jeff', databaseName, '123'.toCharArray()), true)
        readWriteUser = new User(createMongoCRCredential('jeff', databaseName, '123'.toCharArray()), false)
    }

    def cleanup() {
        scheduledExecutorService.shutdown()
    }

    def 'an added user should be found'() {
        given:
        new CreateUserOperation(readOnlyUser, getBufferProvider(), getSession(), true).execute()

        when:
        def found = new UserExistsOperation(databaseName, readOnlyUser.credential.userName, getBufferProvider(), getSession(), true)
                .execute()

        then:
        found

        cleanup:
        new DropUserOperation(databaseName, readOnlyUser.credential.userName, getBufferProvider(), getSession(), true).execute()
    }

    def 'an added user should authenticate'() {
        given:
        new CreateUserOperation(readOnlyUser, getBufferProvider(), getSession(), true).execute()
        def cluster = getCluster()

        when:
        def server = cluster.getServer(new PrimaryServerSelector())
        def connection = server.getConnection()

        then:
        connection

        cleanup:
        new DropUserOperation(databaseName, readOnlyUser.credential.userName, getBufferProvider(), getSession(), true).execute()
        cluster?.close()
    }

    def 'a removed user should not authenticate'() {
        given:
        new CreateUserOperation(readOnlyUser, getBufferProvider(), getSession(), true).execute()
        new DropUserOperation(databaseName, readOnlyUser.credential.userName, getBufferProvider(), getSession(), true).execute()
        def cluster = getCluster()

        when:
        def server = cluster.getServer(new PrimaryServerSelector())
        server.getConnection()

        then:
        thrown(MongoSecurityException)

        cleanup:
        cluster?.close()
    }

    def 'a replaced user should authenticate with its new password'() {
        given:
        new CreateUserOperation(readOnlyUser, getBufferProvider(), getSession(), true).execute()
        def newUser = new User(createMongoCRCredential(readOnlyUser.credential.userName, readOnlyUser.credential.source,
                                                       '234'.toCharArray()), true)
        new UpdateUserOperation(newUser, getBufferProvider(), getSession(), true).execute()
        def cluster = getCluster(newUser)

        when:
        def server = cluster.getServer(new PrimaryServerSelector())
        def connection = server.getConnection()

        then:
        connection

        cleanup:
        new DropUserOperation(databaseName, readOnlyUser.credential.userName, getBufferProvider(), getSession(), true).execute()
        cluster?.close()
    }

    def 'a read write user should be able to write'() {
        given:
        new CreateUserOperation(readWriteUser, getBufferProvider(), getSession(), true).execute()
        def cluster = getCluster()

        when:
        def result = new InsertOperation<Document>(getNamespace(), new Insert<Document>(ACKNOWLEDGED, new Document()), new DocumentCodec(),
                                                   getBufferProvider(), new ClusterSession(cluster, getExecutor()), true).execute()
        then:
        result.commandResult.isOk()

        cleanup:
        new DropUserOperation(databaseName, readOnlyUser.credential.userName, getBufferProvider(), getSession(), true).execute()
        cluster?.close()
    }

    // This test is in UserOperationTest because the assertion is conditional on auth being enabled, and there's no way to do that in Spock
//    def 'a read only user should not be able to write'() {
//        given:
//        new CreateUserOperation(readOnlyUser, getBufferProvider(), getSession(), true).execute()
//        def cluster = createCluster()
//
//        when:
//        new InsertOperation<Document>(getNamespace(), new Insert<Document>(ACKNOWLEDGED, new Document()), new DocumentCodec(),
//                                      getBufferProvider(), new ClusterSession(cluster, getExecutor()), true).execute()
//        then:
//        thrown(MongoWriteException)
//
//        cleanup:
//        new RemoveUserOperation(databaseName, readOnlyUser.credential.userName, getBufferProvider(), getSession(), true).execute()
//        cluster?.close()
//    }

    def getCluster() {
        getCluster(readOnlyUser)
    }

    def getCluster(User user) {
        new DefaultClusterFactory().create(ClusterSettings.builder().hosts(asList(getPrimary())).build(),
                                           ServerSettings.builder().build(),
                                           ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                                           new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                                           new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                                           scheduledExecutorService, asList(user.credential), getBufferProvider(), null, null, null)
    }
}
