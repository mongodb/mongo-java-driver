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

package com.mongodb.operation;

import com.mongodb.FunctionalTest;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;

import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.getSSLSettings;
import static com.mongodb.ClusterFixture.isAuthenticated;
import static com.mongodb.MongoCredential.createMongoCRCredential;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

// This test is here because the assertion is conditional on auth being enabled, and there"s no way to do that in Spock
@SuppressWarnings("unchecked")
public class UserOperationTest extends FunctionalTest {
    private User readOnlyUser;

    @Before
    public void setUp() {
        readOnlyUser = new User(createMongoCRCredential("jeff", getDatabaseName(), "123".toCharArray()), true);
    }

    @Test
    public void readOnlyUserShouldNotBeAbleToWrite() throws Exception {
        assumeTrue(isAuthenticated());

        // given:
        new CreateUserOperation(readOnlyUser).execute(getBinding());
        Cluster cluster = createCluster(readOnlyUser);
        ReadWriteBinding binding = new ClusterBinding(cluster, primary(), 1, SECONDS);

        // when:
        try {
            new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                          asList(new InsertRequest<Document>(new Document())),
                                          new DocumentCodec())
            .execute(binding);
            fail("should have thrown");
        } catch (MongoException e) {
            // all good
        } finally {
            // cleanup:
            new DropUserOperation(getDatabaseName(), readOnlyUser.getCredential().getUserName()).execute(getBinding());
            cluster.close();
        }
    }

    @Test
    public void readWriteAdminUserShouldBeAbleToWriteToADifferentDatabase() throws InterruptedException {
        assumeTrue(isAuthenticated());

        // given
        User adminUser = new User(createMongoCRCredential("jeff-rw-admin", "admin", "123".toCharArray()), false);
        new CreateUserOperation(adminUser).execute(getBinding());

        Cluster cluster = createCluster(adminUser);
        WriteBinding binding = new ClusterBinding(cluster, primary(), 1, SECONDS);

        try {
            // when
            new InsertOperation<Document>(new MongoNamespace(getDatabaseName(), getCollectionName()),
                                          true, ACKNOWLEDGED,
                                          asList(new InsertRequest<Document>(new Document())),
                                          new DocumentCodec()).execute(binding);
            // then
            assertEquals(1L, (long) new CountOperation(new MongoNamespace(getDatabaseName(), getCollectionName()), new Find()
            )
                                    .execute(getBinding()));
        } finally {
            // cleanup
            new DropUserOperation("admin", adminUser.getCredential().getUserName()).execute(getBinding());
            cluster.close();
        }
    }

    @Test
    public void readOnlyAdminUserShouldNotBeAbleToWriteToADatabase() throws InterruptedException {
        assumeTrue(isAuthenticated());
        // given
        User adminUser = new User(createMongoCRCredential("jeff-ro-admin", "admin", "123".toCharArray()), true);
        new CreateUserOperation(adminUser).execute(getBinding());

        Cluster cluster = createCluster(adminUser);
        WriteBinding binding = new ClusterBinding(cluster, primary(), 1, SECONDS);

        try {
            // when
            new InsertOperation<Document>(new MongoNamespace(getDatabaseName(), getCollectionName()), true, ACKNOWLEDGED,
                                          asList(new InsertRequest<Document>(new Document())),
                                          new DocumentCodec()).execute(binding);
            fail("Should have thrown");
        } catch (MongoException e) {
            // all good
        } finally {
            // cleanup
            new DropUserOperation("admin", adminUser.getCredential().getUserName()).execute(getBinding());
            cluster.close();
        }
    }

    @Test
    public void readOnlyAdminUserShouldBeAbleToReadFromADifferentDatabase() throws InterruptedException {
        assumeTrue(isAuthenticated());

        // given
        User adminUser = new User(createMongoCRCredential("jeff-ro-admin", "admin", "123".toCharArray()), true);
        new CreateUserOperation(adminUser).execute(getBinding());

        Cluster cluster = createCluster(adminUser);
        ReadWriteBinding binding = new ClusterBinding(cluster, primary(), 1, SECONDS);
        try {
            // when
            long result = new CountOperation(new MongoNamespace(getDatabaseName(), getCollectionName()),
                                             new Find()
            )
                          .execute(binding);
            // then
            assertEquals(0, result);
        } finally {
            // cleanup
            new DropUserOperation("admin", adminUser.getCredential().getUserName()).execute(getBinding());
            cluster.close();
        }
    }


    private Cluster createCluster(final User user) throws InterruptedException {
        return new DefaultClusterFactory().create(ClusterSettings.builder().hosts(asList(getPrimary())).build(),
                                                  ServerSettings.builder().build(),
                                                  ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                                                  new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                                                  new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                                                  asList(user.getCredential()),
                                                  null, null, null);
    }

}
