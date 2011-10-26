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

import com.mongodb.ReplicaSetStatus.Node;
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
    }
}

