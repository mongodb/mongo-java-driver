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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.util.Collection;

// See https://github.com/mongodb/specifications/tree/master/source/read-write-concern/tests/
@RunWith(Parameterized.class)
public class ReadConcernConnectionStringTest extends TestCase {
    private final String description;
    private final String input;
    private final BsonDocument definition;

    public ReadConcernConnectionStringTest(@SuppressWarnings("unused") final String fileName, final String description,
            final String input, final BsonDocument definition) {
        this.description = description;
        this.input = input;
        this.definition = definition;
    }

    @Test
    public void shouldPassAllOutcomes() {
        boolean valid = definition.getBoolean("valid", BsonBoolean.TRUE).getValue();
        try {
            ConnectionString connectionString = new ConnectionString(input);
            assertTrue(valid);
            ReadConcern readConcern = connectionString.getReadConcern() != null ? connectionString.getReadConcern() : ReadConcern.DEFAULT;
            assertEquals(definition.getDocument("readConcern"), readConcern.asDocument());
        } catch (IllegalArgumentException e) {
            assertFalse(valid);
        }
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() {
        return JsonPoweredTestHelper.getLegacyTestData("read-write-concern/tests/connection-string/read-concern.json");
    }
}
