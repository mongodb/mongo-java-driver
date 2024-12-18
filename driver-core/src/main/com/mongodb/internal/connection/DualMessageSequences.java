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

import org.bson.BsonBinaryWriter;
import org.bson.BsonElement;
import org.bson.FieldNameValidator;

import java.util.List;

/**
 * Two sequences that may either be coupled or independent.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 */
public abstract class DualMessageSequences extends MessageSequences {

    private final String firstSequenceId;
    private final FieldNameValidator firstFieldNameValidator;
    private final String secondSequenceId;
    private final FieldNameValidator secondFieldNameValidator;

    protected DualMessageSequences(
            final String firstSequenceId,
            final FieldNameValidator firstFieldNameValidator,
            final String secondSequenceId,
            final FieldNameValidator secondFieldNameValidator) {
        this.firstSequenceId = firstSequenceId;
        this.firstFieldNameValidator = firstFieldNameValidator;
        this.secondSequenceId = secondSequenceId;
        this.secondFieldNameValidator = secondFieldNameValidator;
    }

    FieldNameValidator getFirstFieldNameValidator() {
        return firstFieldNameValidator;
    }

    FieldNameValidator getSecondFieldNameValidator() {
        return secondFieldNameValidator;
    }

    String getFirstSequenceId() {
        return firstSequenceId;
    }

    String getSecondSequenceId() {
        return secondSequenceId;
    }

    protected abstract EncodeDocumentsResult encodeDocuments(WritersProviderAndLimitsChecker writersProviderAndLimitsChecker);

    /**
     * @see #tryWrite(WriteAction)
     */
    public interface WritersProviderAndLimitsChecker {
        /**
         * Provides writers to the specified {@link WriteAction},
         * {@linkplain WriteAction#doAndGetBatchCount(BsonBinaryWriter, BsonBinaryWriter) executes} it,
         * checks the {@linkplain MessageSettings limits}.
         * <p>
         * May be called multiple times per {@link #encodeDocuments(WritersProviderAndLimitsChecker)}.</p>
         */
        WriteResult tryWrite(WriteAction write);

        /**
         * @see #doAndGetBatchCount(BsonBinaryWriter, BsonBinaryWriter)
         */
        interface WriteAction {
            /**
             * Writes documents to the sequences using the provided writers.
             *
             * @return The resulting batch count since the beginning of {@link #encodeDocuments(WritersProviderAndLimitsChecker)}.
             * It is generally allowed to be greater than {@link MessageSettings#getMaxBatchCount()}.
             */
            int doAndGetBatchCount(BsonBinaryWriter firstWriter, BsonBinaryWriter secondWriter);
        }

        enum WriteResult {
            FAIL_LIMIT_EXCEEDED,
            OK_LIMIT_REACHED,
            OK_LIMIT_NOT_REACHED
        }
    }

    public static final class EncodeDocumentsResult {
        private final boolean serverResponseRequired;
        private final List<BsonElement> extraElements;

        /**
         * @param extraElements See {@link #getExtraElements()}.
         */
        public EncodeDocumentsResult(final boolean serverResponseRequired, final List<BsonElement> extraElements) {
            this.serverResponseRequired = serverResponseRequired;
            this.extraElements = extraElements;
        }

        boolean isServerResponseRequired() {
            return serverResponseRequired;
        }

        /**
         * {@linkplain BsonElement Key/value pairs} to be added to the document contained in the {@code OP_MSG} section with payload type 0.
         */
        List<BsonElement> getExtraElements() {
            return extraElements;
        }
    }
}
