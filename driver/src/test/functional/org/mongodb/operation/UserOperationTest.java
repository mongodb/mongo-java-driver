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

package org.mongodb.operation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoWriteException;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterSettings;
import org.mongodb.connection.ConnectionPoolSettings;
import org.mongodb.connection.DefaultClusterFactory;
import org.mongodb.connection.ServerSettings;
import org.mongodb.connection.SocketSettings;
import org.mongodb.connection.SocketStreamFactory;
import org.mongodb.session.ClusterSession;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getExecutor;
import static org.mongodb.Fixture.getMongoClientURI;
import static org.mongodb.Fixture.getPrimary;
import static org.mongodb.Fixture.getSSLSettings;
import static org.mongodb.Fixture.getSession;
import static org.mongodb.MongoCredential.createMongoCRCredential;
import static org.mongodb.WriteConcern.ACKNOWLEDGED;

// This test is here because the assertion is conditional on auth being enabled, and there's no way to do that in Spock
public class UserOperationTest extends DatabaseTestCase {

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private User readOnlyUser;

    @Before
    public void setUp() {
        super.setUp();
        readOnlyUser = new User(createMongoCRCredential("jeff", getDatabaseName(), "123".toCharArray()), true);
    }

    @After
    public void tearDown() {
        scheduledExecutorService.shutdown();
        super.tearDown();
    }

    @Test
    public void readOnlyUserShouldNotBeAbleToWrite() throws Exception {
        if (getMongoClientURI().getCredentialList().isEmpty()) {
            return;
        }
        // given:
        new CreateUserOperation(readOnlyUser, getBufferProvider(), getSession(), true).execute();
        Cluster cluster = createCluster();

        // when:
        try {
            new InsertOperation<Document>(collection.getNamespace(),
                                          new Insert<Document>(ACKNOWLEDGED, new Document()),
                                          new DocumentCodec(),
                                          getBufferProvider(),
                                          new ClusterSession(cluster, getExecutor()),
                                          true).execute();
            fail("should have throw");
        } catch (MongoWriteException e) {
            // all good
        } finally {
            // cleanup:
            new RemoveUserOperation(getDatabaseName(), readOnlyUser.getCredential().getUserName(), getBufferProvider(), getSession(),
                                    true).execute();
            cluster.close();
        }
    }

    private Cluster createCluster() throws Exception {
        return new DefaultClusterFactory().create(ClusterSettings.builder().hosts(asList(getPrimary())).build(),
                                                  ServerSettings.builder().build(),
                                                  ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                                                  new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                                                  new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                                                  scheduledExecutorService, asList(readOnlyUser.getCredential()), getBufferProvider(),
                                                  null, null, null);
    }

}
