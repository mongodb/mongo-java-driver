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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

// See https://github.com/mongodb/specifications/tree/master/source/read-write-concern/tests/connection-string
@RunWith(Parameterized.class)
public class WriteConcernConnectionStringTest extends TestCase {
    private final String description;
    private final String input;
    private final BsonDocument definition;

    public WriteConcernConnectionStringTest(@SuppressWarnings("unused") final String fileName, final String description,
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
            WriteConcern writeConcern = connectionString.getWriteConcern() != null
                                        ? connectionString.getWriteConcern()
                                        : WriteConcern.ACKNOWLEDGED;
            assertTrue(valid);
            assertEquals(getExpectedWriteConcern(), writeConcern);
        } catch (IllegalArgumentException e) {
            assertFalse(valid);
        }
    }

    private WriteConcern getExpectedWriteConcern() {
        BsonDocument writeConcernDocument = definition.getDocument("writeConcern");

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
    public static Collection<Object[]> data() {
        return JsonPoweredTestHelper.getTestData("/write-concern/connection-string");
    }
}
