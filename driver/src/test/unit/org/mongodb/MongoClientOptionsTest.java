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

package org.mongodb;

import org.junit.Test;
import org.mongodb.codecs.PrimitiveCodecs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoClientOptionsTest {

    @Test
    public void testBuilderDefaults() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        final MongoClientOptions options = builder.build();
        assertEquals(null, options.getDescription());
        assertEquals(WriteConcern.ACKNOWLEDGED, options.getWriteConcern());
        assertEquals(100, options.getMaxConnectionPoolSize());
        assertEquals(10000, options.getConnectTimeout());
        assertEquals(0, options.getMaxAutoConnectRetryTime());
        assertEquals(ReadPreference.primary(), options.getReadPreference());
        assertEquals(5, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertFalse(options.isSocketKeepAlive());
        assertFalse(options.isAutoConnectRetry());
        assertFalse(options.isSSLEnabled());
        assertEquals(AsyncDetector.isAsyncEnabled(), options.isAsyncEnabled());
        assertEquals(5000, options.getHeartbeatFrequency());
        assertEquals(10, options.getHeartbeatConnectRetryFrequency());
        assertEquals(20000, options.getHeartbeatConnectTimeout());
        assertEquals(20000, options.getHeartbeatSocketTimeout());
        assertNull(options.getRequiredReplicaSetName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrimitiveCodecsIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.primitiveCodecs(null);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteConcernIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.writeConcern(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReaPreferenceIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.readPreference(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMinConnectionPoolSizeIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.minConnectionPoolSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxConnectionPoolSizeIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.maxConnectionPoolSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConnectionTimeoutIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.connectTimeout(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxAutoConnectRetryIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.maxAutoConnectRetryTime(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThreadsAllowsToBlockIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.threadsAllowedToBlockForConnectionMultiplier(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxConnectionIdleTimeIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.maxConnectionIdleTime(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxConnectionLifeTimeIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.maxConnectionLifeTime(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatFrequencyIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.heartbeatFrequency(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatConnectRetryFrequencyIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.heartbeatConnectRetryFrequency(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatConnectionTimeoutIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.heartbeatConnectTimeout(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatSocketTimeoutIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.heartbeatSocketTimeout(-1);
    }

    @Test
    public void testAsyncEnabledIllegalArguments() {
        if (!AsyncDetector.isAsyncEnabled()) {
            try {
                final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
                builder.asyncEnabled(true);
                fail();
            } catch (IllegalArgumentException e) { // NOPMD
               // all good
            }
        }
    }

    @Test
    public void testBuilderBuild() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.description("test");
        builder.readPreference(ReadPreference.secondary());
        builder.writeConcern(WriteConcern.JOURNALED);
        builder.autoConnectRetry(true);
        builder.minConnectionPoolSize(30);
        builder.maxConnectionPoolSize(600);
        builder.connectTimeout(100);
        builder.maxWaitTime(200);
        builder.maxConnectionIdleTime(300);
        builder.maxConnectionLifeTime(400);
        builder.maxAutoConnectRetryTime(500);
        builder.threadsAllowedToBlockForConnectionMultiplier(1);
        builder.socketKeepAlive(true);
        builder.SSLEnabled(true);
        builder.asyncEnabled(false);
        builder.heartbeatFrequency(5);
        builder.heartbeatConnectRetryFrequency(10);
        builder.heartbeatConnectTimeout(15);
        builder.heartbeatSocketTimeout(20);
        builder.requiredReplicaSetName("test");
        final PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();
        builder.primitiveCodecs(primitiveCodecs);

        final MongoClientOptions options = builder.build();

        assertEquals("test", options.getDescription());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(WriteConcern.JOURNALED, options.getWriteConcern());
        assertEquals(true, options.isAutoConnectRetry());
        assertEquals(30, options.getMinConnectionPoolSize());
        assertEquals(600, options.getMaxConnectionPoolSize());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(200, options.getMaxWaitTime());
        assertEquals(300, options.getMaxConnectionIdleTime());
        assertEquals(400, options.getMaxConnectionLifeTime());
        assertEquals(500, options.getMaxAutoConnectRetryTime());
        assertEquals(1, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(true, options.isSocketKeepAlive());
        assertTrue(options.isSSLEnabled());
        assertFalse(options.isAsyncEnabled());
        assertSame(primitiveCodecs, options.getPrimitiveCodecs());
        assertEquals(5, options.getHeartbeatFrequency());
        assertEquals(10, options.getHeartbeatConnectRetryFrequency());
        assertEquals(15, options.getHeartbeatConnectTimeout());
        assertEquals(20, options.getHeartbeatSocketTimeout());
        assertEquals("test", options.getRequiredReplicaSetName());
    }
}
