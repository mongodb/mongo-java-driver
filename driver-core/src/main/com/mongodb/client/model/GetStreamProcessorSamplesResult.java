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

package com.mongodb.client.model;

import org.bson.BsonDocument;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The result of a {@code StreamProcessor#getStreamProcessorSamples} call.
 *
 * <p>Callers MUST check {@link #getCursorId()}: a value of {@code 0} means the cursor is
 * exhausted and no further calls should be made.</p>
 *
 * @since 5.5
 */
public class GetStreamProcessorSamplesResult {
    private final long cursorId;
    private final List<BsonDocument> documents;

    /**
     * Constructs an instance.
     *
     * @param cursorId  the cursor ID; {@code 0} indicates the cursor is exhausted
     * @param documents the sampled documents returned by this call
     */
    public GetStreamProcessorSamplesResult(final long cursorId, final List<BsonDocument> documents) {
        this.cursorId = cursorId;
        this.documents = Collections.unmodifiableList(notNull("documents", documents));
    }

    /**
     * Gets the cursor ID to pass to the next call.
     *
     * <p>A value of {@code 0} means the cursor is exhausted; callers MUST NOT make further
     * calls with a zero cursor ID.</p>
     *
     * @return the cursor ID
     */
    public long getCursorId() {
        return cursorId;
    }

    /**
     * Gets the batch of sampled documents returned by this call.
     *
     * @return an unmodifiable list of sampled documents
     */
    public List<BsonDocument> getDocuments() {
        return documents;
    }

    @Override
    public String toString() {
        return "GetStreamProcessorSamplesResult{"
                + "cursorId=" + cursorId
                + ", documents=" + documents
                + '}';
    }
}
