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
            fail(String.format("Connection string '%s' should not have throw an exception: %s", input, t));
        }

        MongoCredential credential = connectionString.getCredential();
        if (credential != null) {
            assertString("credential.source", credential.getSource());
            assertString("credential.username", credential.getUserName());
            assertMechanismProperties(credential);

            // Passwords for certain auth mechanisms are ignored.
            String password = credential.getPassword() != null ? new String(credential.getPassword()) : null;
            if (password != null) {
                assertString("credential.password", password);
            }

            assertMechanism("credential.mechanism", credential.getMechanism());
        }
    }

    private void assertString(final String key, final String actual) {
        BsonValue expected = getExpectedValue(key);

        if (expected.isNull()) {
            assertNull(String.format("%s should be null", key), actual);
        } else if (expected.isString()) {
            String expectedString = expected.asString().getValue();
            assertEquals(String.format("%s should be %s but was %s", key, actual, expectedString), actual, expectedString);
        } else {
            fail(String.format("%s should be %s but was %s", key, actual, expected));
        }
    }

    private void assertMechanism(final String key, final String actual) {
        BsonValue expected = getExpectedValue(key);

        // MONGODB-CR was removed from the AuthenticationMechanism enum for the 4.0 release, so null will be assigned.
        if (expected.isString() && expected.asString().getValue().equals("MONGODB-CR")) {
            assertNull(String.format("%s should be null when the expected mechanism is MONGODB-CR", key), actual);
        } else {
            assertString(key, actual);
        }
    }

    private void assertMechanismProperties(final MongoCredential credential) {
        BsonValue expected = getExpectedValue("credential.mechanism_properties");

        if (!expected.isNull()) {
            BsonDocument document = expected.asDocument();
            for (String key : document.keySet()) {
                if (document.get(key).isString()) {
                    String expectedValue = document.getString(key).getValue();

                    // If the mechanism is "GSSAPI", the default SERVICE_NAME, which is stated as "mongodb" in the specification,
                    // is set to null in the driver.
                    if (credential.getMechanism().equals("GSSAPI") && key.equals("SERVICE_NAME") && expectedValue.equals("mongodb")) {
                        assertNull(credential.getMechanismProperty(key, null));
                    } else {
                        assertEquals(expectedValue, credential.getMechanismProperty(key, null));
                    }
                } else {
                    assertEquals(document.getBoolean(key).getValue(), credential.getMechanismProperty(key, (Boolean) null).booleanValue());
                }
            }
        }
    }

    private BsonValue getExpectedValue(final String key) {
        BsonValue expected = definition;
        if (key.contains(".")) {
            for (String subKey : key.split("\\.")) {
                expected = expected.asDocument().get(subKey);
            }
        } else {
            expected = expected.asDocument().get(key);
        }
        return expected;
    }
}
