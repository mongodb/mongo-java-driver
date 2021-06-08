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

package com.mongodb.client;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * See https://github.com/mongodb/specifications/blob/master/source/crud/tests/README.rst#prose-tests
 */
public class CrudProseTest extends DatabaseTestCase {
    private BsonDocument failPointDocument;

    @Before
    @Override
    public void setUp() {
        super.setUp();
    }

    /**
     * 1. WriteConcernError.details exposes writeConcernError.errInfo
     */
    @Test
    public void testWriteConcernErrInfoIsPropagated() {
        assumeTrue(isDiscoverableReplicaSet() && serverVersionAtLeast(4, 0));

        try {
            setFailPoint();
            collection.insertOne(Document.parse("{ x: 1 }"));
        } catch (MongoWriteConcernException e) {
            assertEquals(e.getWriteConcernError().getCode(), 100);
            assertEquals("UnsatisfiableWriteConcern", e.getWriteConcernError().getCodeName());
            assertEquals(e.getWriteConcernError().getDetails(), new BsonDocument("writeConcern",
                    new BsonDocument("w", new BsonInt32(2))
                            .append("wtimeout", new BsonInt32(0))
                            .append("provenance", new BsonString("clientSupplied"))));
        } catch (Exception ex) {
            fail(format("Incorrect exception thrown in test: %s", ex.getClass()));
        } finally {
            disableFailPoint();
        }
    }

    /**
     * 2. WriteError.details exposes writeErrors[].errInfo
     */
    @Test
    public void testWriteErrorDetailsIsPropagated() {
        assumeTrue(serverVersionAtLeast(3, 2));

        getCollectionHelper().create(getCollectionName(),
                new CreateCollectionOptions()
                        .validationOptions(new ValidationOptions()
                                .validator(Filters.type("x", "string"))));

        try {
            collection.insertOne(new Document("x", 1));
            fail("Should throw, as document doesn't match schema");
        } catch (MongoWriteException e) {
            // These assertions doesn't do exactly what's required by the specification, but it's simpler to implement and nearly as
            // effective
            assertTrue(e.getMessage().contains("Write error"));
            assertNotNull(e.getError().getDetails());
            if (serverVersionAtLeast(5, 0)) {
                assertFalse(e.getError().getDetails().isEmpty());
            }
        }

        try {
            collection.insertMany(asList(new Document("x", 1)));
            fail("Should throw, as document doesn't match schema");
        } catch (MongoBulkWriteException e) {
            // These assertions doesn't do exactly what's required by the specification, but it's simpler to implement and nearly as
            // effective
            assertTrue(e.getMessage().contains("Write errors"));
            assertEquals(1, e.getWriteErrors().size());
            if (serverVersionAtLeast(5, 0)) {
                assertFalse(e.getWriteErrors().get(0).getDetails().isEmpty());
            }
        }
    }

    private void setFailPoint() {
        failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument("times", new BsonInt32(1)))
                .append("data", new BsonDocument("failCommands", new BsonArray(asList(new BsonString("insert"))))
                        .append("writeConcernError", new BsonDocument("code", new BsonInt32(100))
                                .append("codeName", new BsonString("UnsatisfiableWriteConcern"))
                                .append("errmsg", new BsonString("Not enough data-bearing nodes"))
                                .append("errInfo", new BsonDocument("writeConcern", new BsonDocument("w", new BsonInt32(2))
                                        .append("wtimeout", new BsonInt32(0))
                                        .append("provenance", new BsonString("clientSupplied"))))));
        getCollectionHelper().runAdminCommand(failPointDocument);
    }

    private void disableFailPoint() {
        getCollectionHelper().runAdminCommand(failPointDocument.append("mode", new BsonString("off")));
    }
}
