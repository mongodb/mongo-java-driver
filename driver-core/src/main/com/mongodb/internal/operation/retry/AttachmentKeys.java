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
package com.mongodb.internal.operation.retry;

import com.mongodb.annotations.Immutable;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.internal.async.function.LoopState.AttachmentKey;
import com.mongodb.internal.operation.MixedBulkWriteOperation.BulkWriteTracker;
import org.bson.BsonDocument;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;

/**
 * A class with {@code static} methods providing access to {@link AttachmentKey}s relevant when implementing retryable operations.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @see AttachmentKey
 */
public final class AttachmentKeys {
    private static final AttachmentKey<Integer> MAX_WIRE_VERSION = new DefaultAttachmentKey<>("maxWireVersion");
    private static final AttachmentKey<BsonDocument> COMMAND = new DefaultAttachmentKey<>("command");
    private static final AttachmentKey<Boolean> RETRYABLE_COMMAND_FLAG = new DefaultAttachmentKey<>("retryableCommandFlag");
    private static final AttachmentKey<Supplier<String>> COMMAND_DESCRIPTION_SUPPLIER = new DefaultAttachmentKey<>(
            "commandDescriptionSupplier");
    private static final AttachmentKey<BulkWriteTracker> BULK_WRITE_TRACKER = new DefaultAttachmentKey<>("bulkWriteTracker");
    private static final AttachmentKey<BulkWriteResult> BULK_WRITE_RESULT = new DefaultAttachmentKey<>("bulkWriteResult");

    public static AttachmentKey<Integer> maxWireVersion() {
        return MAX_WIRE_VERSION;
    }

    public static AttachmentKey<BsonDocument> command() {
        return COMMAND;
    }

    public static AttachmentKey<Boolean> retryableCommandFlag() {
        return RETRYABLE_COMMAND_FLAG;
    }

    public static AttachmentKey<Supplier<String>> commandDescriptionSupplier() {
        return COMMAND_DESCRIPTION_SUPPLIER;
    }

    public static AttachmentKey<BulkWriteTracker> bulkWriteTracker() {
        return BULK_WRITE_TRACKER;
    }

    public static AttachmentKey<BulkWriteResult> bulkWriteResult() {
        return BULK_WRITE_RESULT;
    }

    private AttachmentKeys() {
        fail();
    }

    @Immutable
    private static final class DefaultAttachmentKey<V> implements AttachmentKey<V> {
        private static final Set<String> AVOID_KEY_DUPLICATION = new HashSet<>();

        private final String key;

        private DefaultAttachmentKey(final String key) {
            assertTrue(AVOID_KEY_DUPLICATION.add(key));
            this.key = key;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DefaultAttachmentKey<?> that = (DefaultAttachmentKey<?>) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public String toString() {
            return key;
        }
    }
}
