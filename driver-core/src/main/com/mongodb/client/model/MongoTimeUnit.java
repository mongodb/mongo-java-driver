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

/**
 * Units for specifying time-based bounds for {@linkplain Window windows} and output units for some time-based
 * {@linkplain WindowedComputation windowed computations}.
 *
 * @since 4.3
 */
public enum MongoTimeUnit {
    /**
     * @mongodb.server.release 5.0
     */
    YEAR("year"),
    /**
     * @mongodb.server.release 5.0
     */
    MONTH("month"),
    /**
     * @mongodb.server.release 5.0
     */
    WEEK("week"),
    /**
     * @mongodb.server.release 5.0
     */
    DAY("day"),
    /**
     * @mongodb.server.release 5.0
     */
    HOUR("hour"),
    /**
     * @mongodb.server.release 5.0
     */
    MINUTE("minute"),
    /**
     * @mongodb.server.release 5.0
     */
    SECOND("second"),
    /**
     * @mongodb.server.release 5.0
     */
    MILLISECOND("millisecond");

    private final String value;

    MongoTimeUnit(final String value) {
        this.value = value;
    }

    String value() {
        return value;
    }
}
