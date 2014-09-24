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

package com.mongodb;

import org.junit.Assert;
import org.junit.Test;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

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
        assertNull(options.getDescription());
        assertEquals(WriteConcern.ACKNOWLEDGED, options.getWriteConcern());
        assertEquals(0, options.getMinConnectionsPerHost());
        assertEquals(100, options.getConnectionsPerHost());
        assertEquals(10000, options.getConnectTimeout());
        assertEquals(ReadPreference.primary(), options.getReadPreference());
        assertEquals(5, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertFalse(options.isSocketKeepAlive());
        assertFalse(options.isSslEnabled());
        assertTrue(options.getSocketFactory() != null);
        assertFalse(options.getSocketFactory() instanceof SSLSocketFactory);
        assertEquals(DefaultDBDecoder.FACTORY, options.getDbDecoderFactory());
        assertEquals(DefaultDBEncoder.FACTORY, options.getDbEncoderFactory());
        assertEquals(0, options.getHeartbeatThreadCount());
        assertEquals(15, options.getAcceptableLatencyDifference());
        assertTrue(options.isCursorFinalizerEnabled());
        assertEquals(10, options.getMinHeartbeatFrequency());
    }

    @Test
    public void testIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();

        try {
            builder.heartbeatFrequency(0);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }
        try {
            builder.minHeartbeatFrequency(0);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }
        try {
            builder.writeConcern(null);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }
        try {
            builder.readPreference(null);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }
        try {
            builder.connectionsPerHost(0);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }
        try {
            builder.minConnectionsPerHost(-1);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }
        try {
            builder.connectTimeout(-1);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }
        try {
            builder.threadsAllowedToBlockForConnectionMultiplier(0);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }

        try {
            builder.dbDecoderFactory(null);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }

        try {
            builder.dbEncoderFactory(null);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // NOPMD all good
        }

    }


    @Test
    public void testBuilderBuild() {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.description("test");
        builder.readPreference(ReadPreference.secondary());
        builder.writeConcern(WriteConcern.JOURNAL_SAFE);
        builder.minConnectionsPerHost(30);
        builder.connectionsPerHost(500);
        builder.connectTimeout(100);
        builder.maxWaitTime(200);
        builder.maxConnectionIdleTime(300);
        builder.maxConnectionLifeTime(400);
        builder.threadsAllowedToBlockForConnectionMultiplier(1);
        builder.socketKeepAlive(true);
        builder.sslEnabled(true);
        builder.dbDecoderFactory(LazyDBDecoder.FACTORY);
        builder.heartbeatFrequency(5);
        builder.minHeartbeatFrequency(11);
        builder.heartbeatConnectTimeout(15);
        builder.heartbeatSocketTimeout(20);
        builder.heartbeatThreadCount(4);
        builder.acceptableLatencyDifference(25);
        builder.requiredReplicaSetName("test");
        builder.cursorFinalizerEnabled(false);

        DBEncoderFactory encoderFactory = new MyDBEncoderFactory();
        builder.dbEncoderFactory(encoderFactory);

        MongoClientOptions options = builder.build();

        assertEquals("test", options.getDescription());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(WriteConcern.JOURNAL_SAFE, options.getWriteConcern());
        assertEquals(200, options.getMaxWaitTime());
        assertEquals(300, options.getMaxConnectionIdleTime());
        assertEquals(400, options.getMaxConnectionLifeTime());
        assertEquals(30, options.getMinConnectionsPerHost());
        assertEquals(500, options.getConnectionsPerHost());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(1, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertTrue(options.isSocketKeepAlive());
        assertTrue(options.isSslEnabled());
        assertEquals(LazyDBDecoder.FACTORY, options.getDbDecoderFactory());
        assertEquals(encoderFactory, options.getDbEncoderFactory());
        assertEquals(5, options.getHeartbeatFrequency());
        assertEquals(11, options.getMinHeartbeatFrequency());
        assertEquals(15, options.getHeartbeatConnectTimeout());
        assertEquals(20, options.getHeartbeatSocketTimeout());
        assertEquals(4, options.getHeartbeatThreadCount());
        assertEquals(25, options.getAcceptableLatencyDifference());
        assertEquals("test", options.getRequiredReplicaSetName());
        assertFalse(options.isCursorFinalizerEnabled());

        assertEquals(5, options.getServerSettings().getHeartbeatFrequency(MILLISECONDS));
        assertEquals(11, options.getServerSettings().getMinHeartbeatFrequency(MILLISECONDS));
        assertEquals(4, options.getServerSettings().getHeartbeatThreadCount());
    }

    @Test
    public void testSocketFactory() {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        SocketFactory socketFactory = SSLSocketFactory.getDefault();
        builder.socketFactory(socketFactory);
        assertTrue(builder.build().getSocketFactory() == socketFactory);

        builder.sslEnabled(false);
        assertTrue(builder.build().getSocketFactory() != null);
        assertFalse(builder.build().getSocketFactory() instanceof SSLSocketFactory);

        builder.sslEnabled(true);
        assertTrue(builder.build().getSocketFactory() instanceof SSLSocketFactory);
    }

    private static class MyDBEncoderFactory implements DBEncoderFactory {
        @Override
        public DBEncoder create() {
            return new DefaultDBEncoder();
        }
    }
}
