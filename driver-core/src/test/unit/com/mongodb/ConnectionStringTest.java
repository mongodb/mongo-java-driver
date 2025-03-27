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

import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.util.Collection;

import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/connection-string/tests
public class ConnectionStringTest extends AbstractConnectionStringTest {
    public ConnectionStringTest(final String filename, final String description, final String input, final BsonDocument definition) {
        super(filename, description, input, definition);
    }

    @Test
    public void shouldPassAllOutcomes() {
        // Java driver currently throws an IllegalArgumentException for these tests
        assumeFalse(getDescription().equals("Empty integer option values are ignored"));
        assumeFalse(getDescription().equals("Comma in a key value pair causes a warning"));

        if (getFilename().equals("invalid-uris.json")) {
            testInvalidUris();
        } else if (getFilename().equals("valid-auth.json")) {
            testValidAuth();
        } else if (getFilename().equals("valid-db-with-dotted-name.json")) {
            testValidHostIdentifiers();
            testValidAuth();
        } else if (getFilename().equals("valid-host_identifiers.json")) {
            testValidHostIdentifiers();
        } else if (getFilename().equals("valid-options.json")) {
            testValidOptions();
        } else if (getFilename().equals("valid-unix_socket-absolute.json")) {
            testValidHostIdentifiers();
        } else if (getFilename().equals("valid-unix_socket-relative.json")) {
            testValidHostIdentifiers();
        } else if (getFilename().equals("valid-warnings.json")) {
            testValidHostIdentifiers();
            if (!getDefinition().get("options").isNull()) {
                testValidOptions();
            }
        } else {
            throw new IllegalArgumentException("Unsupported file: " + getFilename());
        }
    }


    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return JsonPoweredTestHelper.getTestData("/connection-string");
    }
}
