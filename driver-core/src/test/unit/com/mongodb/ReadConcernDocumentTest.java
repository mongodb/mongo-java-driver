/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

// See https://github.com/mongodb/specifications/tree/master/source/read-write-concern/tests/connection-string
@RunWith(Parameterized.class)
public class ReadConcernDocumentTest extends TestCase {
    private final String description;
    private final BsonDocument definition;

    public ReadConcernDocumentTest(final String description, final BsonDocument definition) {
        this.description = description;
        this.definition = definition;
    }

    @Test
    public void shouldPassAllOutcomes() {
        boolean valid = definition.getBoolean("valid", BsonBoolean.TRUE).getValue();
        try {
            ReadConcern readConcern = getReadConcern(definition.getDocument("readConcern"));
            ReadConcern expectedReadConcern = getReadConcern(definition.getDocument("readConcernDocument"));

            // just a sanity check of the tests. These should be equal by definition
            assertEquals(expectedReadConcern, readConcern);
            assertEquals(definition.getBoolean("isServerDefault").getValue(), readConcern.isServerDefault());
            assertTrue(valid);
        } catch (IllegalArgumentException e) {
            assertFalse(valid);
        }
    }

    private ReadConcern getReadConcern(final BsonDocument readConcernDocument) {
        ReadConcern readConcern;
        if (readConcernDocument.containsKey("level")) {
            readConcern = new ReadConcern(ReadConcernLevel.fromString(readConcernDocument.getString("level").getValue()));
        } else {
            readConcern = ReadConcern.DEFAULT;
        }

        return readConcern;
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/read-concern/document")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{test.asDocument().getString("description").getValue(),
                                      test.asDocument()});
            }
        }
        return data;
    }
}
