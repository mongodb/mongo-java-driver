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

package com.mongodb;

import java.util.Arrays;
import java.util.List;

/**
 * A categorization of errors returned by a MongoDB server command.
 *
 * @since 3.0
 */
public enum ErrorCategory {
    /**
     * An uncategorized error
     */
    UNCATEGORIZED,

    /**
     * A duplicate key error
     *
     * @mongodb.driver.manual core/index-unique/ Unique Indexes
     */
    DUPLICATE_KEY,

    /**
     * An execution timeout error
     *
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ maxTimeMS
     */
    EXECUTION_TIMEOUT;

    private static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);
    private static final List<Integer> EXECUTION_TIMEOUT_ERROR_CODES = Arrays.asList(50);

    /**
     * Translate an error code into an error category
     *
     * @param code the error code
     * @return the error category for the given code
     */
    public static ErrorCategory fromErrorCode(final int code) {
        if (DUPLICATE_KEY_ERROR_CODES.contains(code)) {
            return DUPLICATE_KEY;
        } else if (EXECUTION_TIMEOUT_ERROR_CODES.contains(code)) {
            return EXECUTION_TIMEOUT;
        } else {
            return UNCATEGORIZED;
        }
    }
}
