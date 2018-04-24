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
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonString;
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
import java.util.concurrent.TimeUnit;

// See https://github.com/mongodb/specifications/tree/master/source/read-write-concern/tests/connection-string
@RunWith(Parameterized.class)
public class WriteConcernDocumentTest extends TestCase {
    private final String description;
    private final BsonDocument writeConcernDocument;
    private final BsonDocument definition;

    public WriteConcernDocumentTest(final String description, final BsonDocument writeConcernDocument, final BsonDocument definition) {
        this.description = description;
        this.writeConcernDocument = writeConcernDocument;
        this.definition = definition;
    }

    @Test
    public void shouldPassAllOutcomes() {
        boolean valid = definition.getBoolean("valid", BsonBoolean.TRUE).getValue();
        try {
            WriteConcern writeConcern = getWriteConcern(writeConcernDocument);
            assertTrue(valid);
            assertEquals(writeConcern.isAcknowledged(), definition.getBoolean("isAcknowledged").getValue());
            assertEquals(writeConcern.isServerDefault(), definition.getBoolean("isServerDefault").getValue());
            assertEquals(writeConcern.asDocument(), definition.getDocument("writeConcernDocument"));
        } catch (IllegalArgumentException e) {
            assertFalse(valid);
        }
    }

    private WriteConcern getWriteConcern(final BsonDocument writeConcernDocument) {
        BsonValue wValue = writeConcernDocument.get("w");
        WriteConcern retVal;
        if (wValue == null) {
            retVal = WriteConcern.ACKNOWLEDGED;
        } else if (wValue instanceof BsonNumber) {
            retVal = new WriteConcern(wValue.asNumber().intValue());
        } else if (wValue instanceof BsonString) {
            retVal = new WriteConcern(wValue.asString().getValue());
        } else {
            throw new IllegalArgumentException("Unexpected w value: " + wValue);
        }

        if (writeConcernDocument.containsKey("wtimeoutMS")) {
            retVal = retVal.withWTimeout(writeConcernDocument.getNumber("wtimeoutMS", new BsonInt32(0)).intValue(), TimeUnit.MILLISECONDS);
        }
        if (writeConcernDocument.containsKey("journal")) {
            retVal = retVal.withJournal(writeConcernDocument.getBoolean("journal", BsonBoolean.FALSE).getValue());
        }
        return retVal;
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/write-concern/document")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{test.asDocument().getString("description").getValue(),
                                      test.asDocument().getDocument("writeConcern"),
                                      test.asDocument()});
            }
        }
        return data;
    }
}
