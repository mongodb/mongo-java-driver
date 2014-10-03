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
import java.net.UnknownHostException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoClientURITest {

    @Test
    public void testUnsupportedOption() {
        new MongoClientURI("mongodb://localhost/?unknownOption=true");
    }

    @Test
    public void testURIGetter() {
       assertEquals("mongodb://localhost", new MongoClientURI("mongodb://localhost").getURI());
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
        assertEquals(asList("db.example.com"), u.getHosts());
        assertNull(u.getDatabase());
        assertNull(u.getCollection());
        assertNull( u.getUsername());
        assertEquals(null, u.getPassword());
    }

    @Test()
    public void testWithDatabase() {
        MongoClientURI u = new MongoClientURI("mongodb://foo/bar");
        assertEquals(asList("foo"), u.getHosts());
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
        assertEquals(asList("foo"), u.getHosts());
        assertEquals("bar", u.getDatabase());
        assertEquals("goo", u.getCollection());
    }

    @Test()
    public void testUserPass() {
        final String userName = "user";
        final char[] password = "pass".toCharArray();

        MongoClientURI u = new MongoClientURI("mongodb://user:pass@host/bar");
        assertEquals(asList("host"), u.getHosts());
        assertEquals(userName, u.getUsername());
        assertArrayEquals(password, u.getPassword());

        assertEquals(MongoCredential.createCredential(userName, "bar", password), u.getCredentials());

        u = new MongoClientURI("mongodb://user@host/?authMechanism=GSSAPI");
        assertEquals(MongoCredential.createGSSAPICredential(userName), u.getCredentials());

        u = new MongoClientURI("mongodb://user@host/?authMechanism=GSSAPI&gssapiServiceName=foo");
        assertEquals(MongoCredential.createGSSAPICredential(userName).withMechanismProperty("SERVICE_NAME", "foo"), u.getCredentials());

        u = new MongoClientURI("mongodb://user:pass@host/?authMechanism=MONGODB-CR");
        assertEquals(MongoCredential.createMongoCRCredential(userName, "admin", password), u.getCredentials());

        u = new MongoClientURI("mongodb://user@host/?authMechanism=MONGODB-X509");
        assertEquals(MongoCredential.createMongoX509Credential(userName), u.getCredentials());

        u = new MongoClientURI("mongodb://bob:pwd@localhost/?authMechanism=PLAIN&authSource=db1");
        assertEquals(MongoCredential.createPlainCredential("bob", "db1", "pwd".toCharArray()), u.getCredentials());

        u = new MongoClientURI("mongodb://bob:pwd@host/?authMechanism=SCRAM-SHA-1");
        assertEquals(MongoCredential.createScramSha1Credential("bob", "admin", "pwd".toCharArray()), u.getCredentials());

        u = new MongoClientURI("mongodb://user:pass@host/?authSource=test");
        assertEquals(MongoCredential.createCredential(userName, "test", password), u.getCredentials());

        u = new MongoClientURI("mongodb://user:pass@host");
        assertEquals(MongoCredential.createCredential(userName, "admin", password), u.getCredentials());
    }

    @Test
    public void testUnsupportedAuthMechanism() {
        try {
            new MongoClientURI("mongodb://user:pass@host/?authMechanism=UNKNOWN");
            fail("Should fail due to unknown authMechanism");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testURIEncoding() {
        MongoClientURI u = new MongoClientURI("mongodb://use%24:he%21%21o@localhost");
        assertEquals(MongoCredential.createCredential("use$", "admin", "he!!o".toCharArray()), u.getCredentials());
    }

    @Test()
    public void testUserPassAndPort() {
        MongoClientURI u = new MongoClientURI("mongodb://user:pass@host:27011/bar");
        assertEquals("user", u.getUsername());
        assertEquals("pass", new String(u.getPassword()));
        assertEquals(asList("host:27011"), u.getHosts());
    }

    @Test()
    public void testUserPassAndMultipleHostsWithPort() {
        MongoClientURI u = new MongoClientURI("mongodb://user:pass@host:27011,host2:27012,host3:27013/bar");
        assertEquals("user", u.getUsername());
        assertEquals("pass", new String(u.getPassword()));
        assertEquals(asList("host2:27012" , "host3:27013", "host:27011"), u.getHosts());
    }

    @Test()
    public void testWriteConcern() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost");
        assertEquals(WriteConcern.ACKNOWLEDGED, uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?wtimeoutMS=5");
        assertEquals(new WriteConcern(1, 5, false, false), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?fsync=true");
        assertEquals(new WriteConcern(1, 0, true, false), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?j=true");
        assertEquals(new WriteConcern(1, 0, false, true), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?w=2&wtimeoutMS=5&fsync=true&j=true");
        assertEquals(new WriteConcern(2, 5, true, true), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?w=majority&wtimeoutMS=5&fsync=true&j=true");
        assertEquals(new WriteConcern("majority", 5, true, true), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?safe=true");
        assertEquals(WriteConcern.ACKNOWLEDGED, uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?safe=false");
        assertEquals(WriteConcern.UNACKNOWLEDGED, uri.getOptions().getWriteConcern());
    }

    @Test()
    public void testWriteConcernLegacyWtimeout() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost");
        assertEquals(WriteConcern.ACKNOWLEDGED, uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?wtimeout=5");
        assertEquals(new WriteConcern(1, 5, false, false), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?wtimeout=1&wtimeoutms=5");
        assertEquals(new WriteConcern(1, 5, false, false), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?w=2&wtimeout=5&fsync=true&j=true");
        assertEquals(new WriteConcern(2, 5, true, true), uri.getOptions().getWriteConcern());

        uri = new MongoClientURI("mongodb://localhost/?w=majority&wtimeout=5&fsync=true&j=true");
        assertEquals(new WriteConcern("majority", 5, true, true), uri.getOptions().getWriteConcern());
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
                "minPoolSize=7&maxIdleTimeMS=1000&maxLifeTimeMS=2000&" +
                "replicaSet=test&" +
                "connectTimeoutMS=2500&socketTimeoutMS=5500&autoConnectRetry=true&" +
                "slaveOk=true&safe=false&w=1&wtimeout=2500&fsync=true");
        assertOnOptions(uAmp.getOptions());
        MongoClientURI uSemi = new MongoClientURI("mongodb://localhost/?" +
                "maxPoolSize=10;waitQueueMultiple=5;waitQueueTimeoutMS=150;" +
                "minPoolSize=7;maxIdleTimeMS=1000;maxLifeTimeMS=2000;" +
                "replicaSet=test;" +
                "connectTimeoutMS=2500;socketTimeoutMS=5500;autoConnectRetry=true;" +
                "slaveOk=true;safe=false;w=1;wtimeout=2500;fsync=true");
        assertOnOptions(uSemi.getOptions());
        MongoClientURI uMixed = new MongoClientURI("mongodb://localhost/test?" +
                "maxPoolSize=10&waitQueueMultiple=5;waitQueueTimeoutMS=150;" +
                "minPoolSize=7&maxIdleTimeMS=1000;maxLifeTimeMS=2000&" +
                "replicaSet=test;" +
                "connectTimeoutMS=2500;socketTimeoutMS=5500&autoConnectRetry=true;" +
                "slaveOk=true;safe=false&w=1;wtimeout=2500;fsync=true");
        assertOnOptions(uMixed.getOptions());
    }

    @Test
    public void testBuilderOverrides() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost/?maxPoolSize=150",
                MongoClientOptions.builder().autoConnectRetry(true).connectionsPerHost(200));
        assertTrue(uri.getOptions().isAutoConnectRetry());
        assertEquals(150, uri.getOptions().getConnectionsPerHost());
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

    @Test()
    public void testSingleIPV6Server() {
        MongoClientURI u = new MongoClientURI("mongodb://[2010:836B:4179::836B:4179]");
        assertEquals(asList("[2010:836B:4179::836B:4179]"), u.getHosts());
    }

    @Test()
    public void testSingleIPV6ServerWithPort() {
        MongoClientURI u = new MongoClientURI("mongodb://[2010:836B:4179::836B:4179]:1000");
        assertEquals(asList("[2010:836B:4179::836B:4179]:1000"), u.getHosts());
    }

    @Test()
    public void testSingleIPV6ServerWithUserAndPass() {
        MongoClientURI u = new MongoClientURI("mongodb://user:pass@[2010:836B:4179::836B:4179]");
        assertEquals("user", u.getUsername());
        assertArrayEquals("pass".toCharArray(), u.getPassword());
        assertEquals(asList("[2010:836B:4179::836B:4179]"), u.getHosts());
    }

    @Test()
    public void testMultipleIPV6Servers() {
        MongoClientURI u = new MongoClientURI("mongodb://[::1],[2010:836B:4179::836B:4179]");
        assertEquals(asList("[2010:836B:4179::836B:4179]", "[::1]"), u.getHosts());
    }

    @Test()
    public void testMultipleIPV6ServersWithPorts() {
        MongoClientURI u = new MongoClientURI("mongodb://[::1]:1000,[2010:836B:4179::836B:4179]:2000");
        assertEquals(asList("[2010:836B:4179::836B:4179]:2000", "[::1]:1000"), u.getHosts());
    }

    @Test
    public void testSimpleEqualsAndHashCode() {
      List<String> uris = asList(
          "mongodb://user:pass@[2010:836B:4179::836B:4179]",
          "mongodb://localhost/?readPreference=secondaryPreferred",
          "mongodb://[::1]:1000,[2010:836B:4179::836B:4179]:2000",
          "mongodb://localhost/?maxPoolSize=10;waitQueueMultiple=5;waitQueueTimeoutMS=150;"
                  + "minPoolSize=7;maxIdleTimeMS=1000;maxLifeTimeMS=2000;replicaSet=test;"
                  + "connectTimeoutMS=2500;socketTimeoutMS=5500;autoConnectRetry=true;"
                  + "slaveOk=true;safe=false;w=1;wtimeout=2500;fsync=true"
      );
      for (String uri : uris) {
        assertEquals(new MongoClientURI(uri), new MongoClientURI(uri));
        assertEquals(new MongoClientURI(uri).hashCode(), new MongoClientURI(uri).hashCode());
      }
    }

    @Test
    public void testEqualsAndHashCodeWithOptions() {
        MongoClientURI uri = new MongoClientURI("mongodb://user:pass@host1:1,host2:2,host3:3/bar?"
                        + "maxPoolSize=10;waitQueueMultiple=5;waitQueueTimeoutMS=150;"
                        + "minPoolSize=7;maxIdleTimeMS=1000;maxLifeTimeMS=2000;replicaSet=test;"
                        + "connectTimeoutMS=2500;socketTimeoutMS=5500;autoConnectRetry=true;"
                        + "slaveOk=true;safe=false;w=1;wtimeout=2600;fsync=true");

        MongoClientOptions.Builder builder = MongoClientOptions.builder()
                .connectionsPerHost(10)
                .threadsAllowedToBlockForConnectionMultiplier(5)
                .maxWaitTime(150)
                .minConnectionsPerHost(7)
                .maxConnectionIdleTime(1000)
                .maxConnectionLifeTime(2000)
                .requiredReplicaSetName("test")
                .connectTimeout(2500)
                .socketTimeout(5500)
                .autoConnectRetry(true)
                .readPreference(ReadPreference.secondaryPreferred())
                .writeConcern(new WriteConcern(1, 2600, true));

        MongoClientOptions options = builder.build();
        assertEquals(uri.getOptions(), options);

        MongoClientURI uri2 = new MongoClientURI("mongodb://user:pass@host3:3,host1:1,host2:2/bar?", builder);
        assertEquals(uri, uri2);
        assertEquals(uri.hashCode(), uri2.hashCode());
    }

    @SuppressWarnings("deprecation")
    private void assertOnOptions(MongoClientOptions options) {
        assertEquals(10, options.getConnectionsPerHost(), 10);
        assertEquals(7, options.getMinConnectionsPerHost());
        assertEquals(1000, options.getMaxConnectionIdleTime());
        assertEquals(2000, options.getMaxConnectionLifeTime());
        assertEquals(5, options.getThreadsAllowedToBlockForConnectionMultiplier());
        assertEquals(150, options.getMaxWaitTime());
        assertEquals(5500, options.getSocketTimeout());
        assertTrue(options.isAutoConnectRetry());
        assertEquals(new WriteConcern(1, 2500, true), options.getWriteConcern());
        assertEquals(ReadPreference.secondaryPreferred(), options.getReadPreference());
        assertEquals("test", options.getRequiredReplicaSetName());
    }
}
