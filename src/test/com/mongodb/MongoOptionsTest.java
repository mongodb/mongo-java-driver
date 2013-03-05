/**
 * Copyright (C) 2011 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import org.testng.annotations.Test;

import com.mongodb.util.TestCase;

/**
 * The mongo options test.
 */
public class MongoOptionsTest extends TestCase {

    @Test
    @SuppressWarnings("deprecation")
    public void testCopy() throws Exception {

        final MongoOptions options = new MongoOptions();

        options.connectionsPerHost = 100;
        options.threadsAllowedToBlockForConnectionMultiplier = 101;
        options.maxWaitTime = 102;
        options.connectTimeout = 103;
        options.socketTimeout = 104;
        options.socketKeepAlive = true;
        options.autoConnectRetry = true;
        options.maxAutoConnectRetryTime = 105;
        options.slaveOk = true;
        options.safe = true;
        options.w = 106;
        options.wtimeout = 107;
        options.fsync = true;
        options.j = false;
        options.dbDecoderFactory = null;
        options.dbEncoderFactory = null;
        options.socketFactory = null;
        options.description = "cool";
        options.readPreference = ReadPreference.secondary();
        options.cursorFinalizerEnabled = true;
        options.alwaysUseMBeans = true;

        final MongoOptions copy = options.copy();
        assertEquals(options.connectionsPerHost, copy.connectionsPerHost);
        assertEquals(options.threadsAllowedToBlockForConnectionMultiplier, copy.threadsAllowedToBlockForConnectionMultiplier);
        assertEquals(options.maxWaitTime, copy.maxWaitTime);
        assertEquals(options.connectTimeout, copy.connectTimeout);
        assertEquals(options.socketTimeout, copy.socketTimeout);
        assertEquals(options.socketKeepAlive, copy.socketKeepAlive);
        assertEquals(options.autoConnectRetry, copy.autoConnectRetry);
        assertEquals(options.maxAutoConnectRetryTime, copy.maxAutoConnectRetryTime);
        assertEquals(options.slaveOk, copy.slaveOk);
        assertEquals(options.safe, copy.safe);
        assertEquals(options.w, copy.w);
        assertEquals(options.wtimeout, copy.wtimeout);
        assertEquals(options.fsync, copy.fsync);
        assertEquals(options.j, copy.j);
        assertEquals(options.dbDecoderFactory, copy.dbDecoderFactory);
        assertEquals(options.dbEncoderFactory, copy.dbEncoderFactory);
        assertEquals(options.socketFactory, copy.socketFactory);
        assertEquals(options.description, copy.description);
        assertEquals(options.readPreference, copy.readPreference);
        assertEquals(options.alwaysUseMBeans, copy.alwaysUseMBeans);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testGetterSetters() throws Exception {

        final MongoOptions options = new MongoOptions();

        options.setConnectionsPerHost(100);
        options.setThreadsAllowedToBlockForConnectionMultiplier(101);
        options.setMaxWaitTime(102);
        options.setConnectTimeout(103);
        options.setSocketTimeout(104);
        options.setSocketKeepAlive(true);
        options.setAutoConnectRetry(true);
        options.setMaxAutoConnectRetryTime(105);
        options.setSafe(true);
        options.setW(106);
        options.setWtimeout(107);
        options.setFsync(true);
        options.setJ(false);
        options.setDbDecoderFactory(null);
        options.setDbEncoderFactory(null);
        options.setSocketFactory(null);
        options.setDescription("very cool");
        options.setReadPreference(ReadPreference.secondary());
        options.setCursorFinalizerEnabled(true);
        options.setAlwaysUseMBeans(true);

        assertEquals(options.getConnectionsPerHost(), 100);
        assertEquals(options.getThreadsAllowedToBlockForConnectionMultiplier(), 101);
        assertEquals(options.getMaxWaitTime(), 102);
        assertEquals(options.getConnectTimeout(), 103);
        assertEquals(options.getSocketTimeout(), 104);
        assertEquals(options.isSocketKeepAlive(), true);
        assertEquals(options.isAutoConnectRetry(), true);
        assertEquals(options.getMaxAutoConnectRetryTime(), 105);
        assertEquals(options.isSafe(), true);
        assertEquals(options.getW(), 106);
        assertEquals(options.getWtimeout(), 107);
        assertEquals(options.isFsync(), true);
        assertEquals(options.isJ(), false);
        assertEquals(options.getDbDecoderFactory(), null);
        assertEquals(options.getDbEncoderFactory(), null);
        assertEquals(options.getSocketFactory(), null);
        assertEquals(options.getDescription(), "very cool");
        assertEquals(options.getReadPreference(), ReadPreference.secondary());
        assertEquals(options.isCursorFinalizerEnabled(), true);
        assertEquals(options.isAlwaysUseMBeans(), true);
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
        assertEquals(new WriteConcern(0, 3000), options.getWriteConcern());

        options.reset();
        options.fsync = true;
        assertEquals(new WriteConcern(0, 0, true), options.getWriteConcern());

        options.reset();
        options.j = true;
        assertEquals(new WriteConcern(0, 0, false, true), options.getWriteConcern());
    }
}

