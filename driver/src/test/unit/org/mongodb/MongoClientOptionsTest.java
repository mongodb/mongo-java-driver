/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

package org.mongodb;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClientOptions;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MongoClientOptionsTest {

    @Test
    public void testBuilderDefaults() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        MongoClientOptions options = builder.build();
        assertEquals(null, options.getDescription());
        assertEquals(WriteConcern.ACKNOWLEDGED, options.getWriteConcern());
        assertEquals(100, options.getMaxConnectionPoolSize());
        assertEquals(10000, options.getConnectTimeout());
        assertEquals(ReadPreference.primary(), options.getReadPreference());
        assertEquals(5, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertFalse(options.isSocketKeepAlive());
        assertFalse(options.isSSLEnabled());
        assertEquals(10000, options.getHeartbeatFrequency());
        assertEquals(10, options.getHeartbeatConnectRetryFrequency());
        assertEquals(20000, options.getHeartbeatConnectTimeout());
        assertEquals(20000, options.getHeartbeatSocketTimeout());
        assertEquals(0, options.getHeartbeatThreadCount());
        assertEquals(15, options.getAcceptableLatencyDifference());
        assertNull(options.getRequiredReplicaSetName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteConcernIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.writeConcern(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReaPreferenceIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.readPreference(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMinConnectionPoolSizeIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.minConnectionPoolSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxConnectionPoolSizeIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.maxConnectionPoolSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConnectionTimeoutIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.connectTimeout(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThreadsAllowsToBlockIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.threadsAllowedToBlockForConnectionMultiplier(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxConnectionIdleTimeIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.maxConnectionIdleTime(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxConnectionLifeTimeIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.maxConnectionLifeTime(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatFrequencyIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.heartbeatFrequency(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatConnectRetryFrequencyIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.heartbeatConnectRetryFrequency(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatConnectionTimeoutIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.heartbeatConnectTimeout(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatSocketTimeoutIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.heartbeatSocketTimeout(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcceptableLatencyDifferenceIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.acceptableLatencyDifference(-1);
    }

    @Test
    public void testBuilderBuild() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.description("test");
        builder.readPreference(ReadPreference.secondary());
        builder.writeConcern(WriteConcern.JOURNALED);
        builder.minConnectionPoolSize(30);
        builder.maxConnectionPoolSize(600);
        builder.connectTimeout(100);
        builder.maxWaitTime(200);
        builder.maxConnectionIdleTime(300);
        builder.maxConnectionLifeTime(400);
        builder.threadsAllowedToBlockForConnectionMultiplier(1);
        builder.socketKeepAlive(true);
        builder.SSLEnabled(true);
        builder.heartbeatFrequency(5);
        builder.heartbeatConnectRetryFrequency(10);
        builder.heartbeatConnectTimeout(15);
        builder.heartbeatSocketTimeout(20);
        builder.heartbeatThreadCount(4);
        builder.requiredReplicaSetName("test");
        builder.acceptableLatencyDifference(25);

        MongoClientOptions options = builder.build();

        assertEquals("test", options.getDescription());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(WriteConcern.JOURNALED, options.getWriteConcern());
        assertEquals(30, options.getMinConnectionPoolSize());
        assertEquals(600, options.getMaxConnectionPoolSize());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(200, options.getMaxWaitTime());
        assertEquals(300, options.getMaxConnectionIdleTime());
        assertEquals(400, options.getMaxConnectionLifeTime());
        assertEquals(1, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(true, options.isSocketKeepAlive());
        assertTrue(options.isSSLEnabled());
        assertEquals(5, options.getHeartbeatFrequency());
        assertEquals(10, options.getHeartbeatConnectRetryFrequency());
        assertEquals(15, options.getHeartbeatConnectTimeout());
        assertEquals(20, options.getHeartbeatSocketTimeout());
        assertEquals(4, options.getHeartbeatThreadCount());
        assertEquals(25, options.getAcceptableLatencyDifference());
        assertEquals("test", options.getRequiredReplicaSetName());

        assertEquals(5, options.getServerSettings().getHeartbeatFrequency(MILLISECONDS));
        assertEquals(10, options.getServerSettings().getHeartbeatConnectRetryFrequency(MILLISECONDS));
        assertEquals(4, options.getServerSettings().getHeartbeatThreadCount());
    }
}
