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
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// See https://github.com/mongodb/specifications/tree/master/source/connection-string/tests
@RunWith(Parameterized.class)
public class ConnectionStringTest extends TestCase {
    private final String filename;
    private final String description;
    private final String input;
    private final BsonDocument definition;

    public ConnectionStringTest(final String filename, final String description, final String input,
                                final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.input = input;
        this.definition = definition;
    }

    @Test
    public void shouldPassAllOutcomes() {
        if (filename.equals("invalid-uris.json")) {
            testInvalidUris();
        } else if (filename.equals("valid-auth.json")) {
            testValidAuth();
        } else if (filename.equals("valid-db-with-dotted-name.json")) {
            testValidHostIdentifiers();
            testValidAuth();
        } else if (filename.equals("valid-host_identifiers.json")) {
            testValidHostIdentifiers();
        } else if (filename.equals("valid-options.json")) {
            testValidOptions();
        } else if (filename.equals("valid-unix_socket-absolute.json")) {
            testValidHostIdentifiers();
        } else if (filename.equals("valid-unix_socket-relative.json")) {
            testValidHostIdentifiers();
        } else if (filename.equals("valid-warnings.json")) {
            testValidHostIdentifiers();
            if (!definition.get("options").isNull()) {
                testValidOptions();
            }
        } else {
            throw new IllegalArgumentException("Unsupported file: " + filename);
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/connection-string")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        test.asDocument().getString("uri").getValue(), test.asDocument()});
            }
        }
        return data;
    }

    private void testInvalidUris() {
        Throwable expectedError = null;

        try {
            new ConnectionString(input);
        } catch (Throwable t) {
            expectedError = t;
        }

        assertTrue(String.format("Connection string '%s' should have throw an exception", input),
                expectedError instanceof IllegalArgumentException);
    }

    private void testValidHostIdentifiers() {
        ConnectionString connectionString = null;
        try {
            connectionString = new ConnectionString(input);
        } catch (Throwable t) {
            assertTrue(String.format("Connection string '%s' should not have throw an exception: %s", input, t.toString()), false);
        }

        assertExpectedHosts(connectionString.getHosts());
    }

    private void testValidOptions() {
        ConnectionString connectionString = null;

        try {
            connectionString = new ConnectionString(input);
        } catch (Throwable t) {
            assertTrue(String.format("Connection string '%s' should not have throw an exception: %s", input, t.toString()), false);
        }

        for (Map.Entry<String, BsonValue> option : definition.getDocument("options").entrySet()) {
            if (option.getKey().equals("authmechanism")) {
                String expected = option.getValue().asString().getValue();
                String actual = connectionString.getCredential().getAuthenticationMechanism().getMechanismName();
                assertEquals(expected, actual);
            } else if (option.getKey().equals("replicaset")) {
                String expected = option.getValue().asString().getValue();
                assertEquals(expected, connectionString.getRequiredReplicaSetName());
            } else if (option.getKey().equals("wtimeoutms")) {
                int expected = option.getValue().asInt32().getValue();
                assertEquals(expected, connectionString.getWriteConcern().getWTimeout(TimeUnit.MILLISECONDS).intValue());
            } else {
                assertTrue(String.format("Unsupported option '%s' in '%s'", option.getKey(), input), false);
            }
        }
    }

    private void testValidAuth() {
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
