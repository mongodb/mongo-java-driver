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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
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
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
    public void shouldResolve() {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        if (System.getProperty("java.version").startsWith("1.6.")) {
            builder.sslInvalidHostNameAllowed(true);
        }

        if (isError) {
            try {
                new MongoClientURI(this.uri, builder);
                fail();
            } catch (IllegalArgumentException e) {
               // all good
            } catch (MongoClientException e) {
                // all good
            }
        } else {
            MongoClientURI uri = new MongoClientURI(this.uri, builder);

            assertEquals(seeds.size(), uri.getHosts().size());
            assertTrue(uri.getHosts().containsAll(seeds));

            MongoClientOptions mongoClientOptions = uri.getOptions();
            for (Map.Entry<String, BsonValue> entry : options.entrySet()) {
                if (entry.getKey().equals("replicaSet")) {
                    assertEquals(entry.getValue().asString().getValue(), mongoClientOptions.getRequiredReplicaSetName());
                } else if (entry.getKey().equals("ssl")) {
                    assertEquals(entry.getValue().asBoolean().getValue(), mongoClientOptions.isSslEnabled());
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
    public void shouldDiscover() throws InterruptedException {
        if (seeds.isEmpty()) {
            return;
        }
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder()
                .sslInvalidHostNameAllowed(getSslSettings().isInvalidHostNameAllowed());

        MongoClientURI uri = new MongoClientURI(this.uri, optionsBuilder);

        assumeTrue(isDiscoverableReplicaSet() && !serverVersionAtLeast(3, 7)
                && getSslSettings().isEnabled() == uri.getOptions().isSslEnabled());

        MongoClient client = new MongoClient(uri);
        try {
            long startTime = System.currentTimeMillis();
            long currentTime = startTime;
            boolean hostsMatch = false;
            while (currentTime < startTime + TimeUnit.SECONDS.toMillis(5)) {

                List<ServerAddress> currentAddresses = client.getServerAddressList();
                if (currentAddresses.size() == hosts.size() && currentAddresses.containsAll(hosts)) {
                    hostsMatch = true;
                    break;
                }

                Thread.sleep(100);
                currentTime = System.currentTimeMillis();
            }

            assertTrue(hostsMatch);

            assertTrue(client.getDatabase("admin").runCommand(new Document("ping", 1)).containsKey("ok"));
        } finally {
            client.close();
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/initial-dns-seedlist-discovery")) {
            BsonDocument testDocument = util.JsonPoweredTestHelper.getTestDocument(file);
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
