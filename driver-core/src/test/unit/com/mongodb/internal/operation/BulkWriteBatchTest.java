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

package com.mongodb.internal.operation;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BulkWriteBatchTest {
    private static final MongoNamespace NAMESPACE = new MongoNamespace("db.coll");
    private static final ConnectionDescription CONNECTION_DESCRIPTION = new ConnectionDescription(
            new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 6,
            ServerType.REPLICA_SET_PRIMARY, 1000, 16000, 48000, Collections.emptyList());
    private static final OperationContext OPERATION_CONTEXT = ClusterFixture.createOperationContext();

    @ParameterizedTest
    @ValueSource(strings = {
            "{ok: 1, n: 1, writeConcernError: {code: 79, codeName: 'UnknownReplWriteConcern'}}",
            "{ok: 1, n: 1, writeConcernError: {code: 79, codeName: 'UnknownReplWriteConcern', errmsg: null}}"})
    @DisplayName("should report a writeConcernError whose errmsg is missing or null")
    void shouldHandleWriteConcernErrorWithoutErrmsg(final String result) {
        BulkWriteBatch bulkWriteBatch = createBulkWriteBatch();

        bulkWriteBatch.addResult(BsonDocument.parse(result));

        assertTrue(bulkWriteBatch.hasErrors());
        MongoBulkWriteException error = bulkWriteBatch.getError();
        assertNotNull(error);
        assertTrue(error.getWriteErrors().isEmpty());
        assertNotNull(error.getWriteConcernError());
        assertEquals(79, error.getWriteConcernError().getCode());
        assertEquals("", error.getWriteConcernError().getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{ok: 1, n: 0, writeErrors: [{index: 0, code: 100}]}",
            "{ok: 1, n: 0, writeErrors: [{index: 0, code: 100, errmsg: null}]}"})
    @DisplayName("should report a writeError whose errmsg is missing or null")
    void shouldHandleWriteErrorWithoutErrmsg(final String result) {
        BulkWriteBatch bulkWriteBatch = createBulkWriteBatch();

        bulkWriteBatch.addResult(BsonDocument.parse(result));

        assertTrue(bulkWriteBatch.hasErrors());
        MongoBulkWriteException error = bulkWriteBatch.getError();
        assertNotNull(error);
        assertEquals(1, error.getWriteErrors().size());
        assertEquals(100, error.getWriteErrors().get(0).getCode());
        assertEquals("", error.getWriteErrors().get(0).getMessage());
        assertNull(error.getWriteConcernError());
    }

    private static BulkWriteBatch createBulkWriteBatch() {
        return BulkWriteBatch.createBulkWriteBatch(NAMESPACE, CONNECTION_DESCRIPTION, true, WriteConcern.ACKNOWLEDGED, null, false,
                Collections.singletonList(new InsertRequest(BsonDocument.parse("{_id: 1, x: 1}"))), OPERATION_CONTEXT, null, null);
    }
}
