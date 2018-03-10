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

import com.mongodb.lang.Nullable;

/**
 * The options for an unwind aggregation pipeline stage
 *
 * @mongodb.driver.manual reference/operator/aggregation/unwind/ $unwind
 * @mongodb.server.release 3.2
 * @since 3.2
 */
public final class UnwindOptions {

    private Boolean preserveNullAndEmptyArrays;
    private String includeArrayIndex;

    /**
     * If true the unwind stage will include documents that have null values or empty arrays
     *
     * @return the preserve null values and empty arrays value or null
     */
    @Nullable
    public Boolean isPreserveNullAndEmptyArrays() {
        return preserveNullAndEmptyArrays;
    }

    /**
     * Sets true if the unwind stage should include documents that have null values or empty arrays
     *
     * @param preserveNullAndEmptyArrays flag depicting if the unwind stage should include documents that have null values or empty arrays
     * @return this
     */
    public UnwindOptions preserveNullAndEmptyArrays(@Nullable final Boolean preserveNullAndEmptyArrays) {
        this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
        return this;
    }

    /**
     * Gets the includeArrayIndex field if set or null
     *
     * @return the includeArrayIndex field if set or null
     */
    @Nullable
    public String getIncludeArrayIndex() {
        return includeArrayIndex;
    }

    /**
     * Sets the field to be used to store the array index of the unwound item
     *
     * @param arrayIndexFieldName the field to be used to store the array index of the unwound item
     * @return this
     */
    public UnwindOptions includeArrayIndex(@Nullable final String arrayIndexFieldName) {
        this.includeArrayIndex = arrayIndexFieldName;
        return this;
    }

    @Override
    public String toString() {
        return "UnwindOptions{"
                + "preserveNullAndEmptyArrays=" + preserveNullAndEmptyArrays
                + ", includeArrayIndex='" + includeArrayIndex + '\''
                + '}';
    }
}
