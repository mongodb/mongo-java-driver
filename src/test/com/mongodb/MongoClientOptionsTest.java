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

package com.mongodb;


import org.junit.Test;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MongoClientOptionsTest {

    @Test
    public void testBuilderDefaults() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        MongoClientOptions options = builder.build();
        assertEquals(DefaultDBDecoder.FACTORY, options.getDbDecoderFactory());
        assertEquals(DefaultDBEncoder.FACTORY, options.getDbEncoderFactory());
        assertEquals(null, options.getDescription());
        assertEquals(SocketFactory.getDefault(), options.getSocketFactory());
        assertEquals(WriteConcern.ACKNOWLEDGED, options.getWriteConcern());
        assertEquals(100, options.getConnectionsPerHost());
        assertEquals(0, options.getMinConnectionsPerHost());
        assertEquals(0, options.getMaxConnectionIdleTime());
        assertEquals(0, options.getMaxConnectionLifeTime());
        assertEquals(120000, options.getMaxWaitTime());
        assertEquals(10000, options.getConnectTimeout());
        assertEquals(0, options.getMaxAutoConnectRetryTime());
        assertEquals(ReadPreference.primary(), options.getReadPreference());
        assertEquals(5, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(false, options.isSocketKeepAlive());
        assertEquals(true, options.isCursorFinalizerEnabled());
        assertEquals(false, options.isAutoConnectRetry());
        assertEquals(false, options.isAlwaysUseMBeans());
        assertEquals(5000, options.getHeartbeatFrequency());
        assertEquals(10, options.getHeartbeatConnectRetryFrequency());
        assertEquals(20000, options.getHeartbeatConnectTimeout());
        assertEquals(20000, options.getHeartbeatSocketTimeout());
        assertEquals(0, options.getHeartbeatThreadCount());
        assertEquals(getProperty("com.mongodb.slaveAcceptableLatencyMS") != null
                     ? parseInt(getProperty("com.mongodb.slaveAcceptableLatencyMS")) : 15,
                     options.getAcceptableLatencyDifference());
        assertNull(options.getRequiredReplicaSetName());
    }

    @Test
    public void testSystemProperties() {
        try {
            System.setProperty("com.mongodb.updaterIntervalMS", "6000");
            System.setProperty("com.mongodb.updaterIntervalNoMasterMS", "20");
            System.setProperty("com.mongodb.updaterConnectTimeoutMS", "30000");
            System.setProperty("com.mongodb.updaterSocketTimeoutMS", "40000");
            System.setProperty("com.mongodb.slaveAcceptableLatencyMS", "25");

            MongoClientOptions options = MongoClientOptions.builder().build();
            assertEquals(options.getHeartbeatFrequency(), 6000);
            assertEquals(options.getHeartbeatConnectRetryFrequency(), 20);
            assertEquals(options.getHeartbeatConnectTimeout(), 30000);
            assertEquals(options.getHeartbeatSocketTimeout(), 40000);
            assertEquals(options.getAcceptableLatencyDifference(), 25);
        } finally {
            System.setProperty("com.mongodb.updaterIntervalMS", "5000");
            System.setProperty("com.mongodb.updaterIntervalNoMasterMS", "10");
            System.setProperty("com.mongodb.updaterConnectTimeoutMS", "20000");
            System.setProperty("com.mongodb.updaterSocketTimeoutMS", "20000");
            System.setProperty("com.mongodb.slaveAcceptableLatencyMS", "15");
        }
    }


    @Test
    public void testIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        try {
          builder.dbDecoderFactory(null);
          fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.dbEncoderFactory(null);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.socketFactory(null);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.writeConcern(null);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.readPreference(null);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.minConnectionsPerHost(-1);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.connectionsPerHost(0);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.connectTimeout(-1);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.maxAutoConnectRetryTime(-1);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.threadsAllowedToBlockForConnectionMultiplier(0);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.heartbeatFrequency(0);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.heartbeatConnectRetryFrequency(0);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.heartbeatConnectTimeout(-1);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.heartbeatSocketTimeout(-1);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.heartbeatThreadCount(0);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.acceptableLatencyDifference(-1);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            builder.maxConnectionIdleTime(-1);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
        try {
            builder.maxConnectionLifeTime(-1);
            fail("should not get here");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }


    @Test
    public void testBuilderBuild() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.description("test");
        builder.readPreference(ReadPreference.secondary());
        builder.writeConcern(WriteConcern.JOURNAL_SAFE);
        builder.autoConnectRetry(true);
        builder.minConnectionsPerHost(5);
        builder.connectionsPerHost(500);
        builder.maxConnectionIdleTime(500000);
        builder.maxConnectionLifeTime(5000000);
        builder.connectTimeout(100);
        builder.maxAutoConnectRetryTime(300);
        builder.threadsAllowedToBlockForConnectionMultiplier(1);
        builder.socketKeepAlive(true);
        builder.cursorFinalizerEnabled(true);
        builder.alwaysUseMBeans(true);
        builder.acceptableLatencyDifference(41);
        builder.heartbeatFrequency(51);
        builder.heartbeatConnectRetryFrequency(52);
        builder.heartbeatConnectTimeout(53);
        builder.heartbeatSocketTimeout(54);
        builder.heartbeatThreadCount(4);
        builder.requiredReplicaSetName("test");

        SocketFactory socketFactory = SSLSocketFactory.getDefault();
        builder.socketFactory(socketFactory);

        DBEncoderFactory encoderFactory = new DBEncoderFactory() {
            public DBEncoder create() {
                return null;
            }
        };
        builder.dbEncoderFactory(encoderFactory);

        DBDecoderFactory decoderFactory = new DBDecoderFactory() {
            public DBDecoder create() {
                return null;
            }
        };
        builder.dbDecoderFactory(decoderFactory);

        MongoClientOptions options = builder.build();

        assertEquals("test", options.getDescription());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(WriteConcern.JOURNAL_SAFE, options.getWriteConcern());
        assertEquals(true, options.isAutoConnectRetry());
        assertEquals(500, options.getConnectionsPerHost());
        assertEquals(5, options.getMinConnectionsPerHost());
        assertEquals(500000, options.getMaxConnectionIdleTime());
        assertEquals(5000000, options.getMaxConnectionLifeTime());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(300, options.getMaxAutoConnectRetryTime());
        assertEquals(1, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(true, options.isSocketKeepAlive());
        assertEquals(true, options.isCursorFinalizerEnabled());
        assertEquals(true, options.isAlwaysUseMBeans());
        assertEquals(41, options.getAcceptableLatencyDifference());
        assertEquals(51, options.getHeartbeatFrequency());
        assertEquals(52, options.getHeartbeatConnectRetryFrequency());
        assertEquals(53, options.getHeartbeatConnectTimeout());
        assertEquals(54, options.getHeartbeatSocketTimeout());
        assertEquals(4, options.getHeartbeatThreadCount());

        assertEquals(socketFactory, options.getSocketFactory());
        assertEquals(encoderFactory, options.getDbEncoderFactory());
        assertEquals(decoderFactory, options.getDbDecoderFactory());
        assertEquals("test", options.getRequiredReplicaSetName());
    }

    @Test
    public void testStaticBuilderCreate() {
        assertNotNull(MongoClientOptions.builder());
    }

    @Test
    public void testEqualsAndHashCode() {
        assertEquals(MongoClientOptions.builder().build(), MongoClientOptions.builder().build());
        assertEquals(MongoClientOptions.builder().build().hashCode(), MongoClientOptions.builder().build().hashCode());

        assertEquals(MongoClientOptions.builder().socketFactory(SSLSocketFactory.getDefault()).build(),
                MongoClientOptions.builder().socketFactory(SSLSocketFactory.getDefault()).build());
    }
}
