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
import org.mongodb.async.AsyncDetector;
import org.mongodb.serialization.PrimitiveSerializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        assertEquals(100, options.getConnectionsPerHost());
        assertEquals(10000, options.getConnectTimeout());
        assertEquals(0, options.getMaxAutoConnectRetryTime());
        assertEquals(ReadPreference.primary(), options.getReadPreference());
        assertEquals(5, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertFalse(options.isSocketKeepAlive());
        assertFalse(options.isAutoConnectRetry());
        assertFalse(options.isSSLEnabled());
        assertEquals(AsyncDetector.isAsyncEnabled(), options.isAsyncEnabled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrimitiveSerializersIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.primitiveSerializers(null);
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
    public void testConnectionsPerHostIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.connectionsPerHost(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConnectionTimeoutIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.connectTimeout(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxAutoconnectRetryIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.maxAutoConnectRetryTime(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThreadsAllowsToBlockIllegalArguments() {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.threadsAllowedToBlockForConnectionMultiplier(0);
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
        builder.connectionsPerHost(500);
        builder.connectTimeout(100);
        builder.maxAutoConnectRetryTime(300);
        builder.threadsAllowedToBlockForConnectionMultiplier(1);
        builder.socketKeepAlive(true);
        builder.SSLEnabled(true);
        builder.asyncEnabled(false);
        final PrimitiveSerializers primitiveSerializers = PrimitiveSerializers.createDefault();
        builder.primitiveSerializers(primitiveSerializers);

        final MongoClientOptions options = builder.build();

        assertEquals("test", options.getDescription());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(WriteConcern.JOURNALED, options.getWriteConcern());
        assertEquals(true, options.isAutoConnectRetry());
        assertEquals(500, options.getConnectionsPerHost());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(300, options.getMaxAutoConnectRetryTime());
        assertEquals(1, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(true, options.isSocketKeepAlive());
        assertTrue(options.isSSLEnabled());
        assertFalse(options.isAsyncEnabled());
        assertSame(primitiveSerializers, options.getPrimitiveSerializers());
    }
}
