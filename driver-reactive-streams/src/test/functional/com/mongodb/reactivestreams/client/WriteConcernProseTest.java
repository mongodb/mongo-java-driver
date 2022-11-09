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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/change-streams/tests/README.rst#prose-tests
public class WriteConcernProseTest extends DatabaseTestCase {
    private BsonDocument failPointDocument;
    private CollectionHelper<Document> collectionHelper;

    @Before
    @Override
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();
        collectionHelper = new CollectionHelper<>(new DocumentCodec(), new MongoNamespace(getDefaultDatabaseName(), "test"));
    }

    // Ensure that the WriteConcernError errInfo object is propagated.
    @Test
    public void testWriteConcernErrInfoIsPropagated() {
        try {
            setFailPoint();
            insertOneDocument();
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

    private void insertOneDocument() {
        Mono.from(collection.insertOne(Document.parse("{ x: 1 }"))).block(TIMEOUT_DURATION);
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
        collectionHelper.runAdminCommand(failPointDocument);
    }

    private void disableFailPoint() {
        collectionHelper.runAdminCommand(failPointDocument.append("mode", new BsonString("off")));
    }

    private boolean canRunTests() {
        return isDiscoverableReplicaSet() && serverVersionAtLeast(4, 0);
    }
}
