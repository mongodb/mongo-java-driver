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

package com.mongodb.internal.connection;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DescriptionHelperTracingTest {
    private static final ConnectionId CONNECTION_ID =
            new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()));

    private static BsonDocument hello(final boolean tracing) {
        BsonDocument doc = BsonDocument.parse(
                "{ ok: 1, ismaster: true, maxWireVersion: 25, minWireVersion: 0,"
                        + " maxBsonObjectSize: 16777216, maxMessageSizeBytes: 48000000, maxWriteBatchSize: 100000 }");
        if (tracing) {
            doc.put("tracingSupport", BsonBoolean.TRUE);
        }
        return doc;
    }

    @Test
    void parsesTracingSupportTrue() {
        ConnectionDescription description = DescriptionHelper.createConnectionDescription(
                ClusterConnectionMode.SINGLE, CONNECTION_ID, hello(true));
        assertTrue(description.isTracingSupported());
    }

    @Test
    void defaultsTracingSupportFalseWhenAbsent() {
        ConnectionDescription description = DescriptionHelper.createConnectionDescription(
                ClusterConnectionMode.SINGLE, CONNECTION_ID, hello(false));
        assertFalse(description.isTracingSupported());
    }

    @Test
    void parsesTracingSupportFalseWhenExplicitlyFalse() {
        BsonDocument helloResult = hello(false);
        helloResult.put("tracingSupport", BsonBoolean.FALSE);
        ConnectionDescription description = DescriptionHelper.createConnectionDescription(
                ClusterConnectionMode.SINGLE, CONNECTION_ID, helloResult);
        assertFalse(description.isTracingSupported());
    }

    @Test
    void withTracingSupportPreservesOtherFields() {
        ConnectionDescription original = DescriptionHelper.createConnectionDescription(
                ClusterConnectionMode.SINGLE, CONNECTION_ID, hello(false));
        ConnectionDescription updated = original.withTracingSupport(true);

        assertTrue(updated.isTracingSupported());
        assertEquals(original.getConnectionId(), updated.getConnectionId());
        assertEquals(original.getMaxWireVersion(), updated.getMaxWireVersion());
        assertEquals(original.getServerType(), updated.getServerType());
    }
}
