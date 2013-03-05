// MongoURITest.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import javax.net.SocketFactory;
import java.util.Arrays;


@SuppressWarnings("deprecation")
public class MongoURITest extends TestCase {

    @Test
    public void testGetters() {
        MongoURI mongoURI = new MongoURI( "mongodb://user:pwd@localhost/test.mongoURITest?safe=false");
        assertEquals("user", mongoURI.getUsername());
        assertEquals("pwd", new String(mongoURI.getPassword()));
        assertEquals(MongoCredential.createMongoCRCredential("user", "test", "pwd".toCharArray()), mongoURI.getCredentials());
        assertEquals(Arrays.asList("localhost"), mongoURI.getHosts());
        assertEquals("test", mongoURI.getDatabase());
        assertEquals("mongoURITest", mongoURI.getCollection());
        assertEquals(WriteConcern.NORMAL, mongoURI.getOptions().writeConcern);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testOptionDefaults() {
        MongoURI mongoURI = new MongoURI( "mongodb://localhost");
        MongoOptions options = mongoURI.getOptions();

        assertEquals(options.getConnectionsPerHost(), 10);
        assertEquals(options.getThreadsAllowedToBlockForConnectionMultiplier(), 5);
        assertEquals(options.getMaxWaitTime(), 120000);
        assertEquals(options.getConnectTimeout(), 10000);
        assertEquals(options.getSocketTimeout(), 0);
        assertEquals(options.isSocketKeepAlive(), false);
        assertEquals(options.isAutoConnectRetry(), false);
        assertEquals(options.getMaxAutoConnectRetryTime(), 0);
        assertEquals(options.isSafe(), false);
        assertEquals(options.getW(), 0);
        assertEquals(options.getWtimeout(), 0);
        assertEquals(options.isFsync(), false);
        assertEquals(options.isJ(), false);
        assertEquals(options.getDbDecoderFactory(), DefaultDBDecoder.FACTORY);
        assertEquals(options.getDbEncoderFactory(), DefaultDBEncoder.FACTORY);
        assertEquals(options.getSocketFactory(), SocketFactory.getDefault());
        assertEquals(options.getDescription(), null);
        assertEquals(options.getReadPreference(), ReadPreference.primary());
        assertEquals(options.getWriteConcern(), WriteConcern.NORMAL);
        assertEquals(options.slaveOk, false);
        assertEquals(options.isCursorFinalizerEnabled(), true);
    }

    @Test
    public void testOptionSameInstance() {
        MongoURI mongoURI = new MongoURI( "mongodb://localhost");
        assertSame(mongoURI.getOptions(), mongoURI.getOptions());
    }
}
