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

package com.mongodb.client;

import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.SslSettings;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.Before;
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
import static com.mongodb.ClusterFixture.isLoadBalanced;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/initial-dns-seedlist-discovery/tests
@RunWith(Parameterized.class)
public class InitialDnsSeedlistDiscoveryTest {

    private final String filename;
    private final String parentDirectory;
    private final String uri;
    private final List<String> seeds;
    private final List<ServerAddress> hosts;
    private final boolean isError;
    private final BsonDocument options;

    public InitialDnsSeedlistDiscoveryTest(final String filename, final String parentDirectory, final String uri, final List<String> seeds,
                                           final List<ServerAddress> hosts, final boolean isError, final BsonDocument options) {
        this.filename = filename;
        this.parentDirectory = parentDirectory;
        this.uri = uri;
        this.seeds = seeds;
        this.hosts = hosts;
        this.isError = isError;
        this.options = options;
    }

    @Before
    public void setUp() {
        if (parentDirectory.equals("replica-set")) {
            assumeTrue(isDiscoverableReplicaSet());
        } else if (parentDirectory.equals("load-balanced")) {
            assumeTrue(isLoadBalanced());
        } else {
            fail("Unexpected parent directory: " + parentDirectory);
        }
    }

    @Test
    public void shouldResolveTxtRecord() throws InterruptedException {

        if (isError) {
            MongoClient client = null;
            try {
                final AtomicReference<MongoException> exceptionReference = new AtomicReference<MongoException>();
                final CountDownLatch latch = new CountDownLatch(1);

                ConnectionString connectionString;
                MongoClientSettings settings;
                try {
                    connectionString = new ConnectionString(uri);
                    final SslSettings sslSettings = getSslSettings(connectionString);
                    assumeTrue("SSL settings don't match", getSslSettings().isEnabled() == sslSettings.isEnabled());
                    settings = MongoClientSettings.builder().applyConnectionString(connectionString)
                            .applyToSslSettings(new Block<SslSettings.Builder>() {
                                @Override
                                public void apply(final SslSettings.Builder builder) {
                                    builder.applySettings(sslSettings);
                                    builder.invalidHostNameAllowed(true);
                                }
                            })
                            .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                                @Override
                                public void apply(final ClusterSettings.Builder builder) {
                                    builder.serverSelectionTimeout(5, TimeUnit.SECONDS);
                                    builder.addClusterListener(new ClusterListener() {
                                        @Override
                                        public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                                            if (event.getNewDescription().getSrvResolutionException() != null) {
                                                exceptionReference.set(event.getNewDescription().getSrvResolutionException());
                                                latch.countDown();
                                            }
                                        }
                                    });
                                }
                            })
                            .build();
                } catch (MongoClientException | IllegalArgumentException e) {
                    // all good
                    return;
                }
                client = MongoClients.create(settings);
                // Load balancing mode has special rules regarding cluster event publishing, so we can't rely on those here.
                // Instead we just try to execute an operation and assert that it throws
                if (settings.getClusterSettings().getMode() == ClusterConnectionMode.LOAD_BALANCED) {
                    try {
                        client.getDatabase("admin").runCommand(new Document("ping", 1));
                    } catch (MongoClientException e) {
                        // all good
                    }
                } else {
                    if (!latch.await(5, TimeUnit.SECONDS)) {
                        fail("Failed to capture SRV resolution exception");
                    }
                    try {
                        throw exceptionReference.get();
                    } catch (MongoClientException e) {
                        // all good
                    }
                }
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
                } else if (entry.getKey().equals("directConnection")) {
                    assertEquals(entry.getValue().asBoolean().getValue(), connectionString.isDirectConnection());
                } else if (entry.getKey().equals("loadBalanced")) {
                    assertEquals(entry.getValue().asBoolean().getValue(), connectionString.isLoadBalanced());
                } else {
                    throw new UnsupportedOperationException("No support configured yet for " + entry.getKey());
                }
            }
        }
    }

    @Test
    public void shouldDiscoverSrvRecord() throws InterruptedException {
        if (seeds.isEmpty()) {
            return;
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final ConnectionString connectionString = new ConnectionString(uri);
        final SslSettings sslSettings = getSslSettings(connectionString);

        assumeTrue("SSL settings don't match", getSslSettings().isEnabled() == sslSettings.isEnabled());

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                    @Override
                    public void apply(final ClusterSettings.Builder builder) {
                        builder.applyConnectionString(connectionString)
                                .addClusterListener(new ClusterListener() {
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
                    }
                })
                .applyToSslSettings(new Block<SslSettings.Builder>() {
                    @Override
                    public void apply(final SslSettings.Builder builder) {
                        builder.applySettings(sslSettings);
                        builder.invalidHostNameAllowed(true);
                    }
                })
                .build();

        MongoClient client = MongoClients.create(settings);

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
                    file.getParentFile().getName(),
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
