/*
 * Copyright 2016 MongoDB, Inc.
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
 * Granularity values for automatic bucketing.
 *
 * @mongodb.driver.manual reference/operator/aggregation/bucketAuto/ $bucketAuto
 * @mongodb.server.release 3.4
 * @link <a href="https://en.wikipedia.org/wiki/Preferred_number">Preferred numbers</a>
 * @since 3.4
 */
public enum BucketGranularity {
    R5,
    R10,
    R20,
    R40,
    R80,
    SERIES_125("1-2-5"),
    E6,
    E12,
    E24,
    E48,
    E96,
    E192,
    POWERSOF2;

    private final String value;

    BucketGranularity() {
        value = name();
    }

    BucketGranularity(final String name) {
        value = name;
    }

    /**
     * Tries find the enum instance for the given value
     *
     * @param value the value to search for
     * @return the enum instance
     */
    public static BucketGranularity fromString(final String value) {
        for (BucketGranularity granularity : BucketGranularity.values()) {
            if (granularity.getValue().equals(value)) {
                return granularity;
            }
        }
        throw new IllegalArgumentException("No Granularity exists for the value " + value);
    }

    /**
     * Returns the display as defined in the preferred number article
     *
     * @return the display name
     */
    public String getValue() {
        return value;
    }
}
