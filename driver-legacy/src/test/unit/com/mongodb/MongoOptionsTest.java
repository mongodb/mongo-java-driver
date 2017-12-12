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

import javax.net.ssl.SSLSocketFactory;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The mongo options test.
 */
public class MongoOptionsTest {

    @Test
    @SuppressWarnings("deprecation")
    public void testCopy() throws Exception {

        MongoOptions options = new MongoOptions();

        options.connectionsPerHost = 100;
        options.threadsAllowedToBlockForConnectionMultiplier = 101;
        options.maxWaitTime = 102;
        options.connectTimeout = 103;
        options.socketTimeout = 104;
        options.socketKeepAlive = true;
        options.safe = true;
        options.w = 106;
        options.wtimeout = 107;
        options.fsync = true;
        options.j = false;
        options.dbDecoderFactory = null;
        options.dbEncoderFactory = null;
        options.description = "cool";
        options.readPreference = ReadPreference.secondary();
        options.cursorFinalizerEnabled = true;
        options.socketFactory = SSLSocketFactory.getDefault();
        options.alwaysUseMBeans = true;
        options.requiredReplicaSetName = "set1";

        MongoOptions copy = options.copy();
        assertEquals(options.connectionsPerHost, copy.connectionsPerHost);
        assertEquals(options.threadsAllowedToBlockForConnectionMultiplier, copy.threadsAllowedToBlockForConnectionMultiplier);
        assertEquals(options.maxWaitTime, copy.maxWaitTime);
        assertEquals(options.connectTimeout, copy.connectTimeout);
        assertEquals(options.socketTimeout, copy.socketTimeout);
        assertEquals(options.socketKeepAlive, copy.socketKeepAlive);
        assertEquals(options.safe, copy.safe);
        assertEquals(options.w, copy.w);
        assertEquals(options.wtimeout, copy.wtimeout);
        assertEquals(options.fsync, copy.fsync);
        assertEquals(options.j, copy.j);
        assertEquals(options.dbDecoderFactory, copy.dbDecoderFactory);
        assertEquals(options.dbEncoderFactory, copy.dbEncoderFactory);
        assertEquals(options.description, copy.description);
        assertEquals(options.readPreference, copy.readPreference);
        assertEquals(options.alwaysUseMBeans, copy.alwaysUseMBeans);
        assertEquals(options.socketFactory, copy.socketFactory);
        assertEquals(options.requiredReplicaSetName, copy.requiredReplicaSetName);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testGetterSetters() throws Exception {

        MongoOptions options = new MongoOptions();

        options.setConnectionsPerHost(100);
        options.setThreadsAllowedToBlockForConnectionMultiplier(101);
        options.setMaxWaitTime(102);
        options.setConnectTimeout(103);
        options.setSocketTimeout(104);
        options.setSocketKeepAlive(true);
        options.setSafe(true);
        options.setW(106);
        options.setWtimeout(107);
        options.setFsync(true);
        options.setJ(false);
        options.setDbDecoderFactory(null);
        options.setDbEncoderFactory(null);
        options.setDescription("very cool");
        options.setReadPreference(ReadPreference.secondary());
        options.setSocketFactory(SSLSocketFactory.getDefault());
        options.setAlwaysUseMBeans(true);
        options.setCursorFinalizerEnabled(false);
        options.requiredReplicaSetName = "set1";

        assertEquals(options.getConnectionsPerHost(), 100);
        assertEquals(options.getThreadsAllowedToBlockForConnectionMultiplier(), 101);
        assertEquals(options.getMaxWaitTime(), 102);
        assertEquals(options.getConnectTimeout(), 103);
        assertEquals(options.getSocketTimeout(), 104);
        assertEquals(options.isSocketKeepAlive(), true);
        assertEquals(options.isSafe(), true);
        assertEquals(options.getW(), 106);
        assertEquals(options.getWtimeout(), 107);
        assertEquals(options.isFsync(), true);
        assertEquals(options.isJ(), false);
        assertEquals(options.getDbDecoderFactory(), null);
        assertEquals(options.getDbEncoderFactory(), null);
        assertEquals(options.getDescription(), "very cool");
        assertEquals(options.getReadPreference(), ReadPreference.secondary());
        assertEquals(options.isAlwaysUseMBeans(), true);
        assertEquals(options.getSocketFactory(), options.socketFactory);
        assertEquals(options.isCursorFinalizerEnabled(), false);
        assertEquals(options.getRequiredReplicaSetName(), "set1");
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testGetWriteConcern() {
        MongoOptions options = new MongoOptions();
        assertEquals(WriteConcern.NORMAL, options.getWriteConcern());

        options.reset();
        options.safe = true;
        assertEquals(WriteConcern.SAFE, options.getWriteConcern());

        options.reset();
        options.w = 3;
        assertEquals(new WriteConcern(3), options.getWriteConcern());

        options.reset();
        options.wtimeout = 3000;
        assertEquals(WriteConcern.ACKNOWLEDGED.withWTimeout(3000, TimeUnit.MILLISECONDS), options.getWriteConcern());

        options.reset();
        options.fsync = true;
        assertEquals(WriteConcern.ACKNOWLEDGED.withFsync(true), options.getWriteConcern());

        options.reset();
        options.j = true;
        assertEquals(WriteConcern.ACKNOWLEDGED.withJournal(true), options.getWriteConcern());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testToClientOptions() throws UnknownHostException {
        MongoOptions options = new MongoOptions();
        options.description = "my client";
        options.fsync = true;
        options.readPreference = ReadPreference.secondary();
        options.requiredReplicaSetName = "test";
        options.cursorFinalizerEnabled = false;
        options.alwaysUseMBeans = true;
        options.connectTimeout = 100;
        options.maxWaitTime = 500;
        options.socketKeepAlive = true;
        options.threadsAllowedToBlockForConnectionMultiplier = 10;

        MongoClientOptions clientOptions = options.toClientOptions();

        assertEquals(options.requiredReplicaSetName, clientOptions.getRequiredReplicaSetName());
        assertEquals(options.description, clientOptions.getDescription());
        assertEquals(WriteConcern.ACKNOWLEDGED.withFsync(true), clientOptions.getWriteConcern());
        assertEquals(0, clientOptions.getMinConnectionsPerHost());
        assertEquals(10, clientOptions.getConnectionsPerHost());
        assertEquals(100, clientOptions.getConnectTimeout());
        assertEquals(500, clientOptions.getMaxWaitTime());
        assertEquals(ReadPreference.secondary(), clientOptions.getReadPreference());
        assertEquals(10, clientOptions.getThreadsAllowedToBlockForConnectionMultiplier());
        assertTrue(clientOptions.isSocketKeepAlive());
        assertFalse(clientOptions.isSslEnabled());
        assertEquals(options.dbDecoderFactory, clientOptions.getDbDecoderFactory());
        assertEquals(options.dbEncoderFactory, clientOptions.getDbEncoderFactory());
        assertEquals(15, clientOptions.getLocalThreshold());
        assertTrue(clientOptions.isAlwaysUseMBeans());
        assertFalse(clientOptions.isCursorFinalizerEnabled());
    }
}

