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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Options related to the creation of time-series collections.
 *
 * @since 4.3
 * @see CreateCollectionOptions
 * @mongodb.driver.manual core/timeseries-collections/ Time-series collections
 */
public class TimeSeriesOptions {
    private final String timeField;
    private String metaField;
    private TimeSeriesGranularity granularity;

    /**
     * Construct a new instance.
     *
     * @param timeField the name of the top-level field to be used for time. Inserted documents must have this field, and the field must be
     *                 of the BSON datetime type.
     */
    public TimeSeriesOptions(final String timeField) {
        this.timeField = notNull("timeField", timeField);
    }

    /**
     * Gets the name of the field holding the time value.
     *
     * @return the name of the field holding the time value.
     */
    public String getTimeField() {
        return timeField;
    }

    /**
     * Gets the name of the meta field.
     *
     * @return the name of the meta field
     */
    @Nullable
    public String getMetaField() {
        return metaField;
    }

    /**
     * Sets the name of the meta field.
     * <p>
     *  The name of the field which contains metadata in each time series document. The metadata in the specified field should be data
     *  that is used to label a unique series of documents. The metadata should rarely, if ever, change.  This field is used to group
     *  related data and may be of any BSON type, except for array. This name may not be the same as the {@code timeField} or "_id".
     * </p>
     * @param metaField the name of the meta field
     * @return this
     */
    public TimeSeriesOptions metaField(@Nullable final String metaField) {
        this.metaField = metaField;
        return this;
    }

    /**
     * Gets the granularity of the time-series data.
     *
     * @return the time-series granularity
     */
    @Nullable
    public TimeSeriesGranularity getGranularity() {
        return granularity;
    }

    /**
     * Sets the granularity of the time-series data.
     *
     * @param granularity the time-series granularity
     * @return this
     */
    public TimeSeriesOptions granularity(@Nullable final TimeSeriesGranularity granularity) {
        this.granularity = granularity;
        return this;
    }

    @Override
    public String toString() {
        return "TimeSeriesOptions{"
                + "timeField='" + timeField + '\''
                + ", metaField='" + metaField + '\''
                + ", granularity=" + granularity
                + '}';
    }
}
