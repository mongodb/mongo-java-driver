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

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * Options related to the creation of time-series collections.
 *
 * @since 4.3
 * @see CreateCollectionOptions
 * @mongodb.driver.manual core/timeseries-collections/ Time-series collections
 */
public final class TimeSeriesOptions {
    private final String timeField;
    private String metaField;
    private TimeSeriesGranularity granularity;
    private Long bucketMaxSpanSeconds;
    private Long bucketRoundingSeconds;

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
     * @see #metaField(String)
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
     * @see #getMetaField()
     */
    public TimeSeriesOptions metaField(@Nullable final String metaField) {
        this.metaField = metaField;
        return this;
    }

    /**
     * Gets the granularity of the time-series data.
     *
     * @return the time-series granularity
     * @see #granularity(TimeSeriesGranularity)
     */
    @Nullable
    public TimeSeriesGranularity getGranularity() {
        return granularity;
    }

    /**
     * Sets the granularity of the time-series data.
     * <p>
     * The default value is {@link TimeSeriesGranularity#SECONDS} if neither {@link #bucketMaxSpan(Long, TimeUnit)} nor
     * {@link #bucketRounding(Long, TimeUnit)} is set. If any of these bucketing options are set, the granularity parameter cannot be set.
     * </p>
     *
     * @param granularity the time-series granularity
     * @return this
     * @see #getGranularity()
     */
    public TimeSeriesOptions granularity(@Nullable final TimeSeriesGranularity granularity) {
        isTrue("granularity is not allowed when bucketMaxSpan is set", bucketMaxSpanSeconds == null);
        isTrue("granularity is not allowed when bucketRounding is set", bucketRoundingSeconds == null);
        this.granularity = granularity;
        return this;
    }

    /**
     * Returns the maximum time span between measurements in a bucket.
     *
     * @param timeUnit the time unit.
     * @return time span between measurements, or {@code null} if not set.
     * @since 4.10
     * @mongodb.server.release 6.3
     * @see #bucketMaxSpan(Long, TimeUnit)
     */
    @Nullable
    public Long getBucketMaxSpan(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (bucketMaxSpanSeconds == null) {
            return null;
        }
        return timeUnit.convert(bucketMaxSpanSeconds, TimeUnit.SECONDS);
    }

    /**
     * Sets the maximum time span between measurements in a bucket.
     * <p>
     * The value of {@code bucketMaxSpan} must be the same as {@link #bucketRounding(Long, TimeUnit)}, which also means that the options
     * must either be both set or both unset. If you set the {@code bucketMaxSpan} parameter, you can't set the granularity parameter.
     * </p>
     *
     * @param bucketMaxSpan time span between measurements. After conversion to seconds using {@link TimeUnit#convert(long, java.util.concurrent.TimeUnit)},
     * the value must be &gt;= 1. {@code null} can be provided to unset any previously set value.
     * @param timeUnit the time unit.
     * @return this
     * @since 4.10
     * @mongodb.server.release 6.3
     * @see #getBucketMaxSpan(TimeUnit)
     */
    public TimeSeriesOptions bucketMaxSpan(@Nullable final Long bucketMaxSpan, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (bucketMaxSpan == null) {
            this.bucketMaxSpanSeconds = null;
        } else {
            isTrue("bucketMaxSpan is not allowed when granularity is set", granularity == null);
            long seconds = TimeUnit.SECONDS.convert(bucketMaxSpan, timeUnit);
            isTrueArgument("bucketMaxSpan, after conversion to seconds, must be >= 1", seconds > 0);
            this.bucketMaxSpanSeconds = seconds;
        }
        return this;
    }

    /**
     * Returns the time interval that determines the starting timestamp for a new bucket.
     *
     * @param timeUnit the time unit.
     * @return the time interval, or {@code null} if not set.
     * @since 4.10
     * @mongodb.server.release 6.3
     * @see #bucketRounding(Long, TimeUnit)
     */
    @Nullable
    public Long getBucketRounding(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (bucketRoundingSeconds == null) {
            return null;
        }
        return timeUnit.convert(bucketRoundingSeconds, TimeUnit.SECONDS);
    }

    /**
     * Specifies the time interval that determines the starting timestamp for a new bucket.
     * <p>
     * The value of {@code bucketRounding} must be the same as {@link #bucketMaxSpan(Long, TimeUnit)}, which also means that the options
     * must either be both set or both unset. If you set the {@code bucketRounding} parameter, you can't set the granularity parameter.
     * </p>
     *
     * @param bucketRounding time interval. After conversion to seconds using {@link TimeUnit#convert(long, java.util.concurrent.TimeUnit)},
     * the value must be &gt;= 1. {@code null} can be provided to unset any previously set value.
     * @param timeUnit the time unit.
     * @return this
     * @since 4.10
     * @mongodb.server.release 6.3
     * @see #getBucketRounding(TimeUnit)
     */
    public TimeSeriesOptions bucketRounding(@Nullable final Long bucketRounding, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (bucketRounding == null) {
            this.bucketRoundingSeconds = null;
        } else {
            isTrue("bucketRounding is not allowed when granularity is set", granularity == null);
            long seconds = TimeUnit.SECONDS.convert(bucketRounding, timeUnit);
            isTrueArgument("bucketRounding, after conversion to seconds, must be >= 1", seconds > 0);
            this.bucketRoundingSeconds = seconds;
        }
        return this;
    }

    @Override
    public String toString() {
        return "TimeSeriesOptions{"
                + "timeField='" + timeField + '\''
                + ", metaField='" + metaField + '\''
                + ", granularity=" + granularity
                + ", bucketMaxSpanSeconds=" + bucketMaxSpanSeconds
                + ", bucketRoundingSeconds=" + bucketRoundingSeconds
                + '}';
    }
}
