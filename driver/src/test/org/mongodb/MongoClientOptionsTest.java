/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb;

import org.junit.Test;
import org.mongodb.serialization.PrimitiveSerializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class MongoClientOptionsTest {

    @Test
    public void testBuilderDefaults() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        MongoClientOptions options = builder.build();
        assertEquals(null, options.getDescription());
        assertEquals(WriteConcern.ACKNOWLEDGED, options.getWriteConcern());
        assertEquals(100, options.getConnectionsPerHost());
        assertEquals(10000, options.getConnectTimeout());
        assertEquals(0, options.getMaxAutoConnectRetryTime());
        assertEquals(ReadPreference.primary(), options.getReadPreference());
        assertEquals(5, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(false, options.isSocketKeepAlive());
        assertEquals(false, options.isAutoConnectRetry());
    }

    @Test
    public void testIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        try {
            builder.primitiveSerializers(null);
            fail();
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.writeConcern(null);
            fail();
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.readPreference(null);
            fail();
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.connectionsPerHost(0);
            fail();
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.connectTimeout(-1);
            fail();
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.maxAutoConnectRetryTime(-1);
            fail();
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.threadsAllowedToBlockForConnectionMultiplier(0);
            fail();
        } catch (IllegalArgumentException e) {
            // all good
        }

    }


    @Test
    public void testBuilderBuild() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.description("test");
        builder.readPreference(ReadPreference.secondary());
        builder.writeConcern(WriteConcern.JOURNALED);
        builder.autoConnectRetry(true);
        builder.connectionsPerHost(500);
        builder.connectTimeout(100);
        builder.maxAutoConnectRetryTime(300);
        builder.threadsAllowedToBlockForConnectionMultiplier(1);
        builder.socketKeepAlive(true);
        final PrimitiveSerializers primitiveSerializers = PrimitiveSerializers.createDefault();
        builder.primitiveSerializers(primitiveSerializers);

        MongoClientOptions options = builder.build();

        assertEquals("test", options.getDescription());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(WriteConcern.JOURNALED, options.getWriteConcern());
        assertEquals(true, options.isAutoConnectRetry());
        assertEquals(500, options.getConnectionsPerHost());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(300, options.getMaxAutoConnectRetryTime());
        assertEquals(1, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(true, options.isSocketKeepAlive());
        assertSame(primitiveSerializers, options.getPrimitiveSerializers());
    }
}
