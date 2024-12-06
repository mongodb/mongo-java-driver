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

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/crud/tests/README.md#prose-tests">CRUD Prose Tests</a>.
 */
final class CrudProseTest extends com.mongodb.client.CrudProseTest {
    @Override
    protected MongoClient createMongoClient(final MongoClientSettings.Builder mongoClientSettingsBuilder) {
        return new SyncMongoClient(MongoClients.create(mongoClientSettingsBuilder.build()));
    }

    @DisplayName("5. MongoClient.bulkWrite collects WriteConcernErrors across batches")
    @Test
    @Override
    protected void testBulkWriteCollectsWriteConcernErrorsAcrossBatches() {
        assumeTrue(java.lang.Boolean.parseBoolean(toString()), "BULK-TODO implement");
    }

    @DisplayName("6. MongoClient.bulkWrite handles individual WriteErrors across batches")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Override
    protected void testBulkWriteHandlesWriteErrorsAcrossBatches(final boolean ordered) {
        assumeTrue(java.lang.Boolean.parseBoolean(toString()), "BULK-TODO implement");
    }

    @DisplayName("8. MongoClient.bulkWrite handles a cursor requiring getMore within a transaction")
    @Test
    @Override
    protected void testBulkWriteHandlesCursorRequiringGetMoreWithinTransaction() {
        assumeTrue(java.lang.Boolean.parseBoolean(toString()), "BULK-TODO implement");
    }

    @DisplayName("11. MongoClient.bulkWrite batch splits when the addition of a new namespace exceeds the maximum message size")
    @Test
    @Override
    protected void testBulkWriteSplitsWhenExceedingMaxMessageSizeBytesDueToNsInfo() {
        assumeTrue(java.lang.Boolean.parseBoolean(toString()), "BULK-TODO implement");
    }

    @DisplayName("12. MongoClient.bulkWrite returns an error if no operations can be added to ops")
    @ParameterizedTest
    @ValueSource(strings = {"document", "namespace"})
    @Override
    protected void testBulkWriteSplitsErrorsForTooLargeOpsOrNsInfo(final String tooLarge) {
        assumeTrue(java.lang.Boolean.parseBoolean(toString()), "BULK-TODO implement");
    }

    @DisplayName("13. MongoClient.bulkWrite returns an error if auto-encryption is configured")
    @Test
    @Override
    protected void testBulkWriteErrorsForAutoEncryption() {
        assumeTrue(java.lang.Boolean.parseBoolean(toString()), "BULK-TODO implement");
    }

    @DisplayName("15. MongoClient.bulkWrite with unacknowledged write concern uses w:0 for all batches")
    @Test
    protected void testWriteConcernOfAllBatchesWhenUnacknowledgedRequested() {
        assumeTrue(java.lang.Boolean.parseBoolean(toString()), "BULK-TODO implement");
    }

    @ParameterizedTest
    @MethodSource("insertMustGenerateIdAtMostOnceArgs")
    @Override
    protected <TDocument> void insertMustGenerateIdAtMostOnce(
            final Class<TDocument> documentClass,
            final boolean expectIdGenerated,
            final Supplier<TDocument> documentSupplier) {
        assumeTrue(java.lang.Boolean.parseBoolean(toString()), "BULK-TODO implement");
    }
}
