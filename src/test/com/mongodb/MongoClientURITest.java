/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import javax.net.SocketFactory;
import java.net.UnknownHostException;
import javax.net.ssl.SSLSocketFactory;

public class MongoClientURITest extends TestCase {

    @Test
    public void testUnsupportedOption() {
        new MongoClientURI("mongodb://localhost/?unknownOption=true");
    }

    @Test
    public void testOptionsWithoutTrailingSlash() {
        try {
            new MongoClientURI("mongodb://localhost?wTimeout=5");
            fail("This is not allowed");
        } catch (IllegalArgumentException e) {
            // ok
        }

    }
    @Test()
    public void testSingleServer() {
        MongoClientURI u = new MongoClientURI("mongodb://db.example.com");
        assertEquals(1, u.getHosts().size());
        assertEquals("db.example.com", u.getHosts().get(0));
        assertNull(u.getDatabase());
        assertNull(u.getCollection());
        assertNull( u.getUsername());
        assertEquals(null, u.getPassword());
    }

    @Test()
    public void testWithDatabase() {
        MongoClientURI u = new MongoClientURI("mongodb://foo/bar");
        assertEquals(1, u.getHosts().size());
        assertEquals("foo", u.getHosts().get(0));
        assertEquals("bar", u.getDatabase());
        assertEquals(null, u.getCollection());
        assertEquals(null, u.getUsername());
        assertEquals(null, u.getPassword());
    }

    @Test()
    public void testWithCollection() {
        MongoClientURI u = new MongoClientURI("mongodb://localhost/test.my.coll");
        assertEquals("test", u.getDatabase());
        assertEquals("my.coll", u.getCollection());
    }

    @Test()
    public void testBasic2() {
        MongoClientURI u = new MongoClientURI("mongodb://foo/bar.goo");
        assertEquals(1, u.getHosts().size());
        assertEquals("foo", u.getHosts().get(0));
        assertEquals("bar", u.getDatabase());
        assertEquals("goo", u.getCollection());
    }

    @Test()
    public void testUserPass() {
        MongoClientURI u = new MongoClientURI("mongodb://user:pass@host/bar");
        assertEquals(1, u.getHosts().size());
        assertEquals("host", u.getHosts().get(0));
        assertEquals("user", u.getUsername());
        assertEquals("pass", new String(u.getPassword()));
        assertEquals(new MongoCredential("user", "pass".toCharArray(), MongoAuthenticationProtocol.NEGOTIATE, "bar"), u.getCredentials());

        u = new MongoClientURI("mongodb://user@host/?authProtocol=GSSAPI");
        assertEquals(new MongoCredential("user", MongoAuthenticationProtocol.GSSAPI), u.getCredentials());

        u = new MongoClientURI("mongodb://user:pass@host/?authSource=test");
        assertEquals(new MongoCredential("user", "pass".toCharArray(), "test"), u.getCredentials());

        u = new MongoClientURI("mongodb://user:pass@host");
        assertEquals(new MongoCredential("user", "pass".toCharArray(), "admin"), u.getCredentials());
    }

    @Test
    public void testURIEncoding() {
        MongoClientURI u = new MongoClientURI("mongodb://use%24:he%21%21o@localhost");
        assertEquals(new MongoCredential("use$", "he!!o".toCharArray()), u.getCredentials());
    }

    @Test()
    public void testUserPassAndPort() {
        MongoClientURI u = new MongoClientURI("mongodb://user:pass@host:27011/bar");
        assertEquals(1, u.getHosts().size());
        assertEquals("host:27011", u.getHosts().get(0));
        assertEquals("user", u.getUsername());
        assertEquals("pass", new String(u.getPassword()));
    }

    @Test()
    public void testUserPassAndMultipleHostsWithPort() {
        MongoClientURI u = new MongoClientURI("mongodb://user:pass@host:27011,host2:27012,host3:27013/bar");
        assertEquals(3, u.getHosts().size());
        assertEquals("host:27011", u.getHosts().get(0));
        assertEquals("host2:27012", u.getHosts().get(1));
        assertEquals("host3:27013", u.getHosts().get(2));
        assertEquals("user", u.getUsername());
        assertEquals("pass", new String(u.getPassword()));
    }

    @Test()
    public void testWriteConcern() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost");
        assertEquals(WriteConcern.ACKNOWLEDGED, uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?wTimeout=5");
        assertEquals(new WriteConcern(1, 5, false, false), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?fsync=true");
        assertEquals(new WriteConcern(1, 0, true, false), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?j=true");
        assertEquals(new WriteConcern(1, 0, false, true), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?w=2&wtimeout=5&fsync=true&j=true");
        assertEquals(new WriteConcern(2, 5, true, true), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?w=majority&wtimeout=5&fsync=true&j=true");
        assertEquals(new WriteConcern("majority", 5, true, true), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?safe=true");
        assertEquals(WriteConcern.ACKNOWLEDGED, uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?safe=false");
        assertEquals(WriteConcern.UNACKNOWLEDGED, uri.getOptions().getWriteConcern());

    }

    @Test
    public void testSSLOption() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost/?ssl=false");
        assertFalse(uri.getOptions().getSocketFactory() instanceof SSLSocketFactory);

        uri = new MongoClientURI("mongodb://localhost/?ssl=true");
        assertTrue(uri.getOptions().getSocketFactory() instanceof SSLSocketFactory);
    }

    @Test()
    public void testOptions() {
        MongoClientURI uAmp = new MongoClientURI("mongodb://localhost/?" +
                "maxPoolSize=10&waitQueueMultiple=5&waitQueueTimeoutMS=150&" +
                "connectTimeoutMS=2500&socketTimeoutMS=5500&autoConnectRetry=true&" +
                "slaveOk=true&safe=false&w=1&wtimeout=2500&fsync=true");
        assertOnOptions(uAmp.getOptions());
        MongoClientURI uSemi = new MongoClientURI("mongodb://localhost/?" +
                "maxPoolSize=10;waitQueueMultiple=5;waitQueueTimeoutMS=150;" +
                "connectTimeoutMS=2500;socketTimeoutMS=5500;autoConnectRetry=true;" +
                "slaveOk=true;safe=false;w=1;wtimeout=2500;fsync=true");
        assertOnOptions(uSemi.getOptions());
        MongoClientURI uMixed = new MongoClientURI("mongodb://localhost/test?" +
                "maxPoolSize=10&waitQueueMultiple=5;waitQueueTimeoutMS=150;" +
                "connectTimeoutMS=2500;socketTimeoutMS=5500&autoConnectRetry=true;" +
                "slaveOk=true;safe=false&w=1;wtimeout=2500;fsync=true");
        assertOnOptions(uMixed.getOptions());
    }

    @Test()
    public void testURIDefaults() throws UnknownHostException {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost");
        MongoClientOptions options = uri.getOptions();

        assertEquals(options.getConnectionsPerHost(), 100);
        assertEquals(options.getThreadsAllowedToBlockForConnectionMultiplier(), 5);
        assertEquals(options.getMaxWaitTime(), 120000);
        assertEquals(options.getConnectTimeout(), 10000);
        assertEquals(options.getSocketTimeout(), 0);
        assertFalse(options.isSocketKeepAlive());
        assertFalse(options.isAutoConnectRetry());
        assertEquals(options.getMaxAutoConnectRetryTime(), 0);
        assertEquals(options.getDbDecoderFactory(), DefaultDBDecoder.FACTORY);
        assertEquals(options.getDbEncoderFactory(), DefaultDBEncoder.FACTORY);
        assertEquals(options.getSocketFactory(), SocketFactory.getDefault());
        assertEquals(options.getDescription(), null);
        assertEquals(options.getReadPreference(), ReadPreference.primary());
        assertTrue(options.isCursorFinalizerEnabled());
        assertNull(uri.getCredentials());
    }

    @Test()
    public void testReadPreferenceOptions() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost/?readPreference=secondaryPreferred");
        assertEquals(ReadPreference.secondaryPreferred(), uri.getOptions().getReadPreference());

        uri = new MongoClientURI("mongodb://localhost/?readPreference=secondaryPreferred&" +
                "readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags=");
        assertEquals(ReadPreference.secondaryPreferred
                (
                        new BasicDBObject("dc", "ny").append("rack", "1"),
                        new BasicDBObject("dc", "ny"),
                        new BasicDBObject()
                ),
                uri.getOptions().getReadPreference());
    }

    @SuppressWarnings("deprecation")
    private void assertOnOptions(MongoClientOptions options) {
        assertEquals(10, options.getConnectionsPerHost(), 10);
        assertEquals(5, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(150, options.getMaxWaitTime());
        assertEquals(5500, options.getSocketTimeout());
        assertTrue(options.isAutoConnectRetry());
        assertEquals(new WriteConcern(1, 2500, true), options.getWriteConcern());
        assertEquals(ReadPreference.secondaryPreferred(), options.getReadPreference());
    }
}
