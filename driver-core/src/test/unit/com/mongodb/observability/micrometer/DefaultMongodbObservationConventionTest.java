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

package com.mongodb.observability.micrometer;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ClusterId;
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultMongodbObservationConventionTest {

    private final DefaultMongodbObservationConvention convention = new DefaultMongodbObservationConvention();

    private static MongodbObservationContext commandContext() {
        MongodbObservationContext ctx = new MongodbObservationContext();
        ctx.setObservationType(MongodbObservation.MONGODB_COMMAND);
        ctx.setCommandName("find");
        ctx.setDatabaseName("db");
        ctx.setCollectionName("coll");
        ctx.setServerAddress(new ServerAddress("localhost", 27017));
        ctx.setConnectionId(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()), 3, 42L));
        ctx.setCursorId(7L);
        ctx.setTransactionNumber(2L);
        ctx.setSessionId("session");
        ctx.setQueryText("{find: 'coll'}");
        return ctx;
    }

    private static Map<String, String> toMap(final KeyValues keyValues) {
        Map<String, String> map = new HashMap<>();
        for (KeyValue kv : keyValues) {
            map.put(kv.getKey(), kv.getValue());
        }
        return map;
    }

    @Test
    void commandLowCardinalityKeyValuesAreComplete() {
        Map<String, String> kv = toMap(convention.getLowCardinalityKeyValues(commandContext()));
        assertEquals("mongodb", kv.get("db.system.name"));
        assertEquals("db", kv.get("db.namespace"));
        assertEquals("coll", kv.get("db.collection.name"));
        assertEquals("find", kv.get("db.command.name"));
        assertEquals("find db.coll", kv.get("db.query.summary"));
        assertEquals("localhost", kv.get("server.address"));
        assertEquals("27017", kv.get("server.port"));
        assertEquals("tcp", kv.get("network.transport"));
        assertEquals(8, kv.size());
    }

    @Test
    void commandLowCardinalityKeyValuesWithoutCollectionOmitCollection() {
        MongodbObservationContext ctx = commandContext();
        ctx.setCollectionName(null);
        Map<String, String> kv = toMap(convention.getLowCardinalityKeyValues(ctx));
        assertFalse(kv.containsKey("db.collection.name"));
        assertEquals("find db", kv.get("db.query.summary"));
    }

    @Test
    void operationLowCardinalityKeyValuesAreComplete() {
        MongodbObservationContext ctx = new MongodbObservationContext();
        ctx.setObservationType(MongodbObservation.MONGODB_OPERATION);
        ctx.setCommandName("insert");
        ctx.setDatabaseName("db");
        ctx.setCollectionName("coll");
        Map<String, String> kv = toMap(convention.getLowCardinalityKeyValues(ctx));
        assertEquals("mongodb", kv.get("db.system.name"));
        assertEquals("db", kv.get("db.namespace"));
        assertEquals("coll", kv.get("db.collection.name"));
        assertEquals("insert", kv.get("db.operation.name"));
        assertEquals("insert db.coll", kv.get("db.operation.summary"));
        assertEquals(5, kv.size());
    }

    @Test
    void commandHighCardinalityKeyValuesAreCompleteWhenRecording() {
        Map<String, String> kv = toMap(convention.getHighCardinalityKeyValues(commandContext()));
        assertEquals("{find: 'coll'}", kv.get("db.query.text"));
        assertEquals("3", kv.get("db.mongodb.driver_connection_id"));
        assertEquals("42", kv.get("db.mongodb.server_connection_id"));
        assertEquals("7", kv.get("db.mongodb.cursor_id"));
        assertEquals("2", kv.get("db.mongodb.txn_number"));
        assertEquals("session", kv.get("db.mongodb.lsid"));
        assertEquals(6, kv.size());
    }

    @Test
    void commandHighCardinalityKeyValuesAreSkippedWhenSpanIsNotRecording() {
        MongodbObservationContext ctx = commandContext();
        ctx.setRecording(false);
        assertTrue(toMap(convention.getHighCardinalityKeyValues(ctx)).isEmpty());
        // low-cardinality values are still produced: metrics handlers on the same registry consume them
        assertEquals(8, toMap(convention.getLowCardinalityKeyValues(ctx)).size());
    }

    @Test
    void recordingDefaultsToTrue() {
        assertTrue(new MongodbObservationContext().isRecording());
    }
}
