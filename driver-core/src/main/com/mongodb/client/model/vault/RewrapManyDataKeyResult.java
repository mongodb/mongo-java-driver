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

package com.mongodb.client.model.vault;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.lang.Nullable;

/**
 * The result of the rewrapping of data keys
 *
 * @since 4.7
 */
public final class RewrapManyDataKeyResult {
    private final BulkWriteResult bulkWriteResult;

    /**
     * Construct a new instance with no bulk write result
     */
    public RewrapManyDataKeyResult() {
        this.bulkWriteResult = null;
    }

    /**
     * Construct a new instance
     * @param bulkWriteResult the bulk write result of the rewrapping data keys
     */
    public RewrapManyDataKeyResult(final BulkWriteResult bulkWriteResult) {
        this.bulkWriteResult = bulkWriteResult;
    }

    /**
     * @return the bulk write result of the rewrapping data keys or null if there was no bulk operation
     */
    @Nullable
    public BulkWriteResult getBulkWriteResult() {
        return bulkWriteResult;
    }
}
