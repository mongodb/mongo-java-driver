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

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.annotations.Immutable;
import com.mongodb.internal.async.function.LoopControl.AttachmentKey;
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
    private static final AttachmentKey<Integer> MAX_WIRE_VERSION = DefaultAttachmentKey.of("maxWireVersion");
    private static final AttachmentKey<BsonDocument> COMMAND = DefaultAttachmentKey.of("command");
    private static final AttachmentKey<Boolean> RETRYABLE_WRITE_COMMAND_FLAG = DefaultAttachmentKey.of("retryableWriteCommandFlag");
    private static final AttachmentKey<Supplier<String>> COMMAND_DESCRIPTION_SUPPLIER = DefaultAttachmentKey.of("commandDescriptionSupplier");

    public static AttachmentKey<Integer> maxWireVersion() {
        return MAX_WIRE_VERSION;
    }

    public static AttachmentKey<BsonDocument> command() {
        return COMMAND;
    }

    /**
     * Setting this flag to {@code false}, or leaving it unset, does not completely disable retrying,
     * but does change which failed results may be eligible for retry.
     * For example, {@link MongoConnectionPoolClearedException} may be eligible for retry regardless of this flag.
     */
    public static AttachmentKey<Boolean> retryableWriteCommandFlag() {
        return RETRYABLE_WRITE_COMMAND_FLAG;
    }

    public static AttachmentKey<Supplier<String>> commandDescriptionSupplier() {
        return COMMAND_DESCRIPTION_SUPPLIER;
    }

    private AttachmentKeys() {
        fail();
    }

    /**
     * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a> class.
     */
    @Immutable
    private static final class DefaultAttachmentKey<V> implements AttachmentKey<V> {
        private static final Set<String> AVOID_KEY_DUPLICATION = new HashSet<>();

        private final String key;

        private DefaultAttachmentKey(final String key) {
            assertTrue(AVOID_KEY_DUPLICATION.add(key));
            this.key = key;
        }

        static <V> DefaultAttachmentKey<V> of(final String key) {
            return new DefaultAttachmentKey<>(key);
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
