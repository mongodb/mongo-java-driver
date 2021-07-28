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
import com.mongodb.MongoClientSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.configureFailPoint;
import static com.mongodb.ClusterFixture.disableFailPoint;
import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.lang.String.format;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.bson.BsonDocument.parse;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class ServerDiscoveryAndMonitoringProseTests {

    static final String HELLO = "hello";
    static final String LEGACY_HELLO = "isMaster";

    @Test
    @SuppressWarnings("try")
    public void testHeartbeatFrequency() throws InterruptedException {
        assumeFalse(isServerlessTest());

        CountDownLatch latch = new CountDownLatch(5);
        MongoClientSettings settings = getMongoClientSettingsBuilder()
                                       .applyToServerSettings(new Block<ServerSettings.Builder>() {
                                           @Override
                                           public void apply(final ServerSettings.Builder builder) {
                                               builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS);
                                               builder.addServerMonitorListener(new ServerMonitorListener() {
                                                   @Override
                                                   public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
                                                       latch.countDown();
                                                   }
                                               });
                                           }
                                       }).build();

        try (MongoClient ignored = MongoClients.create(settings)) {
            assertTrue("Took longer than expected to reach expected number of hearbeats",
                       latch.await(500, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void testRTTUpdates() throws InterruptedException {
        assumeTrue(isStandalone());
        assumeTrue(serverVersionAtLeast(4, 4));

        List<ServerDescriptionChangedEvent> events = synchronizedList(new ArrayList<>());
        MongoClientSettings settings = getMongoClientSettingsBuilder()
                                       .applicationName("streamingRttTest")
                                       .applyToServerSettings(new Block<ServerSettings.Builder>() {
                                           @Override
                                           public void apply(final ServerSettings.Builder builder) {
                                               builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS);
                                               builder.addServerListener(new ServerListener() {
                                                   @Override
                                                   public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
                                                       events.add(event);
                                                   }
                                               });
                                           }
                                       }).build();
        try (MongoClient client = MongoClients.create(settings)) {
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            Thread.sleep(250);
            assertTrue(events.size() >= 1);
            events.forEach(event ->
                           assertTrue(event.getNewDescription().getRoundTripTimeNanos() > 0));

            configureFailPoint(parse(format("{"
                                     + "configureFailPoint: \"failCommand\","
                                     + "mode: {times: 1000},"
                                     + " data: {"
                                     + "   failCommands: [\"%s\", \"%s\"],"
                                     + "   blockConnection: true,"
                                     + "   blockTimeMS: 100,"
                                     + "   appName: \"streamingRttTest\""
                                     + "  }"
                                     + "}", LEGACY_HELLO, HELLO)));

            long startTime = System.currentTimeMillis();
            while (true) {
                long rttMillis = NANOSECONDS.toMillis(client.getClusterDescription().getServerDescriptions().get(0)
                                                      .getRoundTripTimeNanos());
                if (rttMillis > 50) {
                    break;
                }
                assertFalse(System.currentTimeMillis() - startTime > 1000);
                //noinspection BusyWait
                Thread.sleep(50);
            }

        } finally {
            disableFailPoint("failCommand");
        }
    }
}
