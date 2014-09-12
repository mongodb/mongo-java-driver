/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

public class AggregateModel<D> {
    private final List<D> pipeline;
    private Boolean allowDiskUse;
    private Integer batchSize;
    private Long maxTimeMS;
    private Boolean useCursor;

    public AggregateModel(final List<D> pipeline) {
        this.pipeline = pipeline;
    }

    public List<D> getPipeline() {
        return pipeline;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public AggregateModel<D> allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public AggregateModel<D> batchSize(final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is null, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     */
    public Long getMaxTime(final TimeUnit timeUnit) {
        if (maxTimeMS == null) {
            return null;
        }
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time, which may be null
     * @param timeUnit the time unit, which may only be null if maxTime is
     * @return this
     */
    public AggregateModel<D> maxTimeMS(final Long maxTime, final TimeUnit timeUnit) {
        if (maxTime == null) {
            maxTimeMS = null;
        } else {
            notNull("timeUnit", timeUnit);
            this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        }
        return this;
    }

    public Boolean getUseCursor() {
        return useCursor;
    }

    public AggregateModel<D> useCursor(final Boolean useCursor) {
        this.useCursor = useCursor;
        return this;
    }
}
