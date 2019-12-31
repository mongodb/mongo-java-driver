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
import java.util.List;
import java.util.Map;

// See https://github.com/mongodb/specifications/tree/master/source/auth/tests
@RunWith(Parameterized.class)
public class AuthConnectionStringTest extends TestCase {
    private final String filename;
    private final String description;
    private final String input;
    private final BsonDocument definition;

    public AuthConnectionStringTest(final String filename, final String description, final String input,
                                    final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.input = input;
        this.definition = definition;
    }

    @Test
    public void shouldPassAllOutcomes() {
        if (definition.getBoolean("valid").getValue()) {
            testValidUris();
        } else {
            testInvalidUris();
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/auth")) {
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

    private void testValidUris() {
        ConnectionString connectionString = null;

        try {
            connectionString = new ConnectionString(input);
        } catch (Throwable t) {
            assertTrue(String.format("Connection string '%s' should not have throw an exception: %s", input, t.toString()), false);
        }

        MongoCredential credential = connectionString.getCredential();
        assertString("auth.db", credential.getSource());
        assertString("auth.username", credential.getUserName());

        // Passwords for certain auth mechanisms are ignored.
        String password = credential.getPassword() != null ? new String(credential.getPassword()) : null;
        if (password != null) {
            assertString("auth.password", password);
        }
        if (definition.get("options").isDocument()) {
            for (Map.Entry<String, BsonValue> option : definition.getDocument("options").entrySet()) {
                if (option.getKey().equals("authmechanism")) {
                    String expected = option.getValue().asString().getValue();
                    if (expected.equals("MONGODB-CR")) {
                        assertNotNull(connectionString.getCredential());
                        assertNull(connectionString.getCredential().getAuthenticationMechanism());
                    } else {
                        String actual = connectionString.getCredential().getAuthenticationMechanism().getMechanismName();
                        assertEquals(expected, actual);
                    }
                }
            }
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
}
