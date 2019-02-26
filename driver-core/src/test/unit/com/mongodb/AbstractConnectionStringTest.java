/*
 * Copyright 2008-present MongoDB, Inc.
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

import junit.framework.TestCase;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
public abstract class AbstractConnectionStringTest extends TestCase {
    private static final Set<String> UNSUPPORTED_OPTIONS =
            new HashSet<String>(asList(
                    "tlsallowinvalidcertificates",
                    "tlsallowinvalidhostnames",
                    "tlscafile",
                    "tlscertificatekeyfile",
                    "tlscertificatekeyfilepassword",
                    "serverselectiontryonce"));
    private final String filename;
    private final String description;
    private final String input;
    private final BsonDocument definition;

    public AbstractConnectionStringTest(final String filename, final String description, final String input,
                                        final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.input = input;
        this.definition = definition;
    }

    protected String getFilename() {
        return filename;
    }

    protected BsonDocument getDefinition() {
        return definition;
    }

    protected String getDescription() {
        return description;
    }

    protected void testInvalidUris() {
        Throwable expectedError = null;

        try {
            new ConnectionString(input);
        } catch (Throwable t) {
            expectedError = t;
        }

        assertNotNull(String.format("Connection string '%s' should have thrown an exception", input), expectedError);
        assertTrue(String.format("Connection string '%s' should have thrown an IllegalArgumentException", input),
                expectedError instanceof IllegalArgumentException);
    }

    protected void testValidHostIdentifiers() {
        ConnectionString connectionString = null;
        try {
            connectionString = new ConnectionString(input);
        } catch (Throwable t) {
            assertTrue(String.format("Connection string '%s' should not have thrown an exception: %s", input, t.toString()), false);
        }

        assertExpectedHosts(connectionString.getHosts());
    }

    protected void testValidOptions() {
        ConnectionString connectionString = null;

        try {
            connectionString = new ConnectionString(input);
        } catch (Throwable t) {
            assertTrue(String.format("Connection string '%s' should not have thrown an exception: %s", input, t.toString()), false);
        }

        for (Map.Entry<String, BsonValue> option : definition.getDocument("options").entrySet()) {
            if (UNSUPPORTED_OPTIONS.contains(option.getKey().toLowerCase())) {
                continue;
            }

            if (option.getKey().equals("authmechanism")) {
                String expected = option.getValue().asString().getValue();
                String actual = connectionString.getCredential().getAuthenticationMechanism().getMechanismName();
                assertEquals(expected, actual);
            } else if (option.getKey().toLowerCase().equals("retrywrites")) {
                boolean expected = option.getValue().asBoolean().getValue();
                assertEquals(expected, connectionString.getRetryWritesValue().booleanValue());
            } else if (option.getKey().toLowerCase().equals("replicaset")) {
                String expected = option.getValue().asString().getValue();
                assertEquals(expected, connectionString.getRequiredReplicaSetName());
            } else if (option.getKey().toLowerCase().equals("serverselectiontimeoutms")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getServerSelectionTimeout().intValue());
            } else if (option.getKey().toLowerCase().equals("sockettimeoutms")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getSocketTimeout().intValue());
            } else if (option.getKey().equals("wtimeoutms")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getWriteConcern().getWTimeout(TimeUnit.MILLISECONDS).intValue());
            } else if (option.getKey().toLowerCase().equals("connecttimeoutms")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getConnectTimeout().intValue());
            } else if (option.getKey().toLowerCase().equals("heartbeatfrequencyms")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getHeartbeatFrequency().intValue());
            } else if (option.getKey().toLowerCase().equals("localthresholdms")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getLocalThreshold().intValue());
            } else if (option.getKey().toLowerCase().equals("maxidletimems")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getMaxConnectionIdleTime().intValue());
            } else if (option.getKey().toLowerCase().equals("tls")) {
                boolean expected = option.getValue().asBoolean().getValue();
                assertEquals(expected, connectionString.getSslEnabled().booleanValue());
            } else if (option.getKey().toLowerCase().equals("tlsinsecure")) {
                boolean expected = option.getValue().asBoolean().getValue();
                assertEquals(expected, connectionString.getSslInvalidHostnameAllowed().booleanValue());
            } else if (option.getKey().toLowerCase().equals("readconcernlevel")) {
                String expected = option.getValue().asString().getValue();
                assertEquals(expected, connectionString.getReadConcern().getLevel().getValue());
            } else if (option.getKey().toLowerCase().equals("w")) {
                if (option.getValue().isString()) {
                    String expected = option.getValue().asString().getValue();
                    assertEquals(expected, connectionString.getWriteConcern().getWString());
                } else {
                    int expected = option.getValue().asNumber().intValue();
                    assertEquals(expected, connectionString.getWriteConcern().getW());
                }
            } else if (option.getKey().toLowerCase().equals("wtimeoutms")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getWriteConcern().getWTimeout(TimeUnit.MILLISECONDS).intValue());
            } else if (option.getKey().toLowerCase().equals("journal")) {
                boolean expected = option.getValue().asBoolean().getValue();
                assertEquals(expected, connectionString.getWriteConcern().getJournal().booleanValue());
            } else if (option.getKey().toLowerCase().equals("readpreference")) {
                String expected = option.getValue().asString().getValue();
                assertEquals(expected, connectionString.getReadPreference().getName());
            } else if (option.getKey().toLowerCase().equals("readpreferencetags")) {
                BsonArray expected = option.getValue().asArray();
                assertEquals(expected, connectionString.getReadPreference().toDocument().getArray("tags"));
            } else if (option.getKey().toLowerCase().equals("maxstalenessseconds")) {
                int expected = option.getValue().asNumber().intValue();
                assertEquals(expected, connectionString.getReadPreference().toDocument().getNumber("maxStalenessSeconds").intValue());
            } else if (option.getKey().equals("compressors")) {
                BsonArray expectedCompressorList = option.getValue().asArray();
                assertEquals(expectedCompressorList.size(), connectionString.getCompressorList().size());
                for (int i = 0; i < expectedCompressorList.size(); i++) {
                    String expected = expectedCompressorList.get(i).asString().getValue();
                    assertEquals(expected, connectionString.getCompressorList().get(i).getName());
                }
            } else if (option.getKey().toLowerCase().equals("zlibcompressionlevel")) {
                int expected = option.getValue().asNumber().intValue();
                assertEquals(expected, connectionString.getCompressorList().get(0).getProperty("level", 0).intValue());
            } else if (option.getKey().toLowerCase().equals("appname")) {
                String expected = option.getValue().asString().getValue();
                assertEquals(expected, connectionString.getApplicationName());
            } else if (option.getKey().toLowerCase().equals("authmechanism")) {
                String expected = option.getValue().asString().getValue();
                assertEquals(expected, connectionString.getCredential().getMechanism());
            } else if (option.getKey().toLowerCase().equals("authsource")) {
                String expected = option.getValue().asString().getValue();
                assertEquals(expected, connectionString.getCredential().getSource());
            } else if (option.getKey().toLowerCase().equals("authmechanismproperties")) {
                BsonDocument properties = option.getValue().asDocument();
                for (String cur : properties.keySet()) {
                    if (properties.get(cur).isString()) {
                        String expected = properties.getString(cur).getValue();
                        assertEquals(expected, connectionString.getCredential().getMechanismProperty(cur, null));
                    } else {
                        boolean expected = properties.getBoolean(cur).getValue();
                        assertEquals(expected, connectionString.getCredential().getMechanismProperty(cur, (Boolean) null).booleanValue());
                    }
                }
            } else {
                assertTrue(String.format("Unsupported option '%s' in '%s'", option.getKey(), input), false);
            }
        }
    }

    protected void testValidAuth() {
        ConnectionString connectionString = null;

        try {
            connectionString = new ConnectionString(input);
        } catch (Throwable t) {
            if (description.contains("without password")) {
                // We don't allow null passwords without setting the authentication mechanism.
                return;
            } else {
                assertTrue(String.format("Connection string '%s' should not have throw an exception: %s", input, t.toString()), false);
            }
        }

        assertString("auth.db", getAuthDB(connectionString));
        assertString("auth.username", connectionString.getUsername());

        // Passwords for certain auth mechanisms are ignored.
        String password = null;
        if (connectionString.getPassword() != null) {
            password = new String(connectionString.getPassword());
        }
        if (connectionString.getCredential() != null) {
            AuthenticationMechanism mechanism = connectionString.getCredential().getAuthenticationMechanism();
            if (mechanism == null) {
                assertString("auth.password", password);
            } else {
                switch (mechanism) {
                    case PLAIN:
                    case MONGODB_CR:
                    case SCRAM_SHA_1:
                    case SCRAM_SHA_256:
                        assertString("auth.password", password);
                        break;
                    default:
                        // Ignore the password field.
                }
            }

        } else {
            assertString("auth.password", password);
        }
    }

    private void assertString(final String key, final String actual) {
        BsonValue expected = definition;
        if (key.contains(".")) {
            for (String subKey : key.split("\\.")) {
                expected = expected.asDocument().get(subKey);
            }
        } else {
            expected = expected.asDocument().get(key);
        }

        if (expected.isNull()) {
            assertTrue(String.format("%s should be null", key), actual == null);
        } else if (expected.isString()) {
            String expectedString = expected.asString().getValue();
            assertTrue(String.format("%s should be %s but was %s", key, actual, expectedString), actual.equals(expectedString));
        } else {
            assertTrue(String.format("%s should be %s but was %s", key, actual, expected), false);
        }
    }

    private void assertExpectedHosts(final List<String> hosts) {
        List<String> cleanedHosts = new ArrayList<String>();
        for (String host : hosts) {
            if (host.startsWith("[")) {
                int idx = host.indexOf("]");
                cleanedHosts.add(host.substring(1, idx) + host.substring(idx + 1));
            } else {
                cleanedHosts.add(host);
            }
        }


        List<String> expectedHosts = new ArrayList<String>();
        for (BsonValue rawHost : definition.getArray("hosts")) {
            BsonDocument hostDoc = rawHost.asDocument();
            String host = hostDoc.getString("host").getValue();
            String port = "";
            if (!hostDoc.get("port").isNull()) {
                port = ":" + hostDoc.getInt32("port").getValue();
            }
            expectedHosts.add(host + port);
        }
        Collections.sort(expectedHosts);
        assertEquals(expectedHosts, cleanedHosts);
    }

    private String getAuthDB(final ConnectionString connectionString) {
        if (connectionString.getCollection() != null) {
            return connectionString.getDatabase() + "." + connectionString.getCollection();
        }
        return connectionString.getDatabase();
    }
}
