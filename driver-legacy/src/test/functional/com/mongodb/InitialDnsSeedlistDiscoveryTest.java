/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.mongodb;

import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isNotAtLeastJava7;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/initial-dns-seedlist-discovery/tests
@RunWith(Parameterized.class)
public class InitialDnsSeedlistDiscoveryTest {

    private final String filename;
    private final String uri;
    private final List<String> seeds;
    private final List<ServerAddress> hosts;
    private final boolean isError;
    private final BsonDocument options;

    public InitialDnsSeedlistDiscoveryTest(final String filename, final String uri, final List<String> seeds,
                                           final List<ServerAddress> hosts, final boolean isError, final BsonDocument options) {
        this.filename = filename;
        this.uri = uri;
        this.seeds = seeds;
        this.hosts = hosts;
        this.isError = isError;
        this.options = options;
    }

    @Test
    public void shouldResolveTxtRecord() throws InterruptedException {
        assumeFalse(isNotAtLeastJava7());

        if (isError) {
            MongoClient client = null;
            try {
                final AtomicReference<MongoException> exceptionReference = new AtomicReference<MongoException>();
                final CountDownLatch latch = new CountDownLatch(1);
                MongoClientOptions.Builder builder = MongoClientOptions.builder().addClusterListener(
                        new ClusterListener() {
                            @Override
                            public void clusterOpening(final ClusterOpeningEvent event) {
                            }

                            @Override
                            public void clusterClosed(final ClusterClosedEvent event) {
                            }

                            @Override
                            public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                                if (event.getNewDescription().getSrvResolutionException() != null) {
                                    exceptionReference.set(event.getNewDescription().getSrvResolutionException());
                                    latch.countDown();
                                }
                            }
                        });
                client = new MongoClient(new MongoClientURI(uri, builder));
                if (!latch.await(5, TimeUnit.SECONDS)) {
                    fail("");
                }
                throw exceptionReference.get();
            } catch (IllegalArgumentException e) {
                // all good
            } catch (MongoClientException e) {
                // all good
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        } else {
            ConnectionString connectionString = new ConnectionString(this.uri);

            for (Map.Entry<String, BsonValue> entry : options.entrySet()) {
                if (entry.getKey().equals("replicaSet")) {
                    assertEquals(entry.getValue().asString().getValue(), connectionString.getRequiredReplicaSetName());
                } else if (entry.getKey().equals("ssl")) {
                    assertEquals(entry.getValue().asBoolean().getValue(), connectionString.getSslEnabled());
                } else if (entry.getKey().equals("authSource")) {
                    // ignoring authSource for now, because without at least a userName also in the connection string,
                    // the authSource is ignored.  If the test gets this far, at least we know that a TXT record
                    // containing in authSource doesn't blow up.  We just don't test that it's actually used.
                    assertTrue(true);
                } else {
                    throw new UnsupportedOperationException("No support configured yet for " + entry.getKey());
                }
            }
        }
    }

    @Test
    public void shouldDiscoverSrvRecord() throws InterruptedException {
        assumeFalse(isNotAtLeastJava7());
        if (seeds.isEmpty()) {
            return;
        }
        final CountDownLatch latch = new CountDownLatch(1);

        MongoClientOptions.Builder builder = MongoClientOptions.builder().addClusterListener(
                new ClusterListener() {
                    @Override
                    public void clusterOpening(final ClusterOpeningEvent event) {
                    }

                    @Override
                    public void clusterClosed(final ClusterClosedEvent event) {
                    }

                    @Override
                    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                        List<ServerAddress> curHostList = new ArrayList<ServerAddress>();
                        for (ServerDescription cur : event.getNewDescription().getServerDescriptions()) {
                            if (cur.isOk()) {
                                curHostList.add(cur.getAddress());
                            }
                        }
                        if (hosts.size() == curHostList.size() && curHostList.containsAll(hosts)) {
                            latch.countDown();
                        }
                    }
                });
        MongoClientURI mongoClientURI = new MongoClientURI(uri, builder);
        assumeTrue("It's not a replica set", isDiscoverableReplicaSet());
        assumeTrue("SSL settings don't match", getSslSettings().isEnabled() == getSslSettings(mongoClientURI.getProxied()).isEnabled());

        MongoClient client = new MongoClient(mongoClientURI);

        try {
            assertTrue(latch.await(500, TimeUnit.SECONDS));
            assertTrue(client.getDatabase("admin").runCommand(new Document("ping", 1)).containsKey("ok"));
        } finally {
            client.close();
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/initial-dns-seedlist-discovery")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            data.add(new Object[]{
                    file.getName(),
                    testDocument.getString("uri").getValue(),
                    toStringList(testDocument.getArray("seeds")),
                    toServerAddressList(testDocument.getArray("hosts")),
                    testDocument.getBoolean("error", BsonBoolean.FALSE).getValue(),
                    testDocument.getDocument("options", new BsonDocument())
            });

        }
        return data;
    }

    private static List<String> toStringList(final BsonArray bsonArray) {
        List<String> retVal = new ArrayList<String>(bsonArray.size());
        for (BsonValue cur : bsonArray) {
            retVal.add(cur.asString().getValue());
        }
        return retVal;
    }

    private static List<ServerAddress> toServerAddressList(final BsonArray bsonArray) {
        List<ServerAddress> retVal = new ArrayList<ServerAddress>(bsonArray.size());
        for (BsonValue cur : bsonArray) {
            retVal.add(new ServerAddress(cur.asString().getValue()));
        }
        return retVal;
    }
}
