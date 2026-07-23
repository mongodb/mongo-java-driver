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
 * Options for sampling output from a running stream processor.
 *
 * <p>On the first call (when {@code cursorId} is absent or zero), a new sample cursor is opened
 * via {@code startSampleStreamProcessor} and {@code limit} controls the maximum number of
 * documents to sample. On subsequent calls, the cursor is advanced via
 * {@code getMoreSampleStreamProcessor} and {@code batchSize} controls how many documents
 * are returned per call.</p>
 *
 * @since 5.5
 */
public class GetStreamProcessorSamplesOptions {
    @Nullable
    private Long cursorId;
    @Nullable
    private Integer limit;
    @Nullable
    private Integer batchSize;

    /**
     * Constructs an instance with default values.
     */
    public GetStreamProcessorSamplesOptions() {
    }

    /**
     * Gets the cursor ID from a previous call.
     *
     * <p>If absent or zero, a new sample cursor is opened. If non-zero, the next batch
     * is fetched from the existing cursor.</p>
     *
     * @return the cursor ID, or {@code null} if not set
     */
    @Nullable
    public Long getCursorId() {
        return cursorId;
    }

    /**
     * Sets the cursor ID from a previous {@code StreamProcessor#getStreamProcessorSamples} call.
     *
     * @param cursorId the cursor ID; {@code 0} or {@code null} opens a new cursor
     * @return this
     */
    public GetStreamProcessorSamplesOptions cursorId(@Nullable final Long cursorId) {
        this.cursorId = cursorId;
        return this;
    }

    /**
     * Gets the maximum number of documents to sample.
     *
     * <p>Only applied on the initial call when opening a new cursor.</p>
     *
     * @return the limit, or {@code null} if not set
     */
    @Nullable
    public Integer getLimit() {
        return limit;
    }

    /**
     * Sets the maximum number of documents to sample.
     *
     * <p>Only sent on the initial call (i.e. when {@code cursorId} is absent or zero).
     * Ignored on subsequent calls.</p>
     *
     * @param limit the maximum number of documents to sample
     * @return this
     */
    public GetStreamProcessorSamplesOptions limit(@Nullable final Integer limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the number of documents to return per batch.
     *
     * <p>Only applied on subsequent calls when advancing an existing cursor.</p>
     *
     * @return the batch size, or {@code null} if not set
     */
    @Nullable
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * <p>Only sent on subsequent calls (i.e. when {@code cursorId} is non-zero).
     * Ignored on the initial call.</p>
     *
     * @param batchSize the number of documents per batch
     * @return this
     */
    public GetStreamProcessorSamplesOptions batchSize(@Nullable final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public String toString() {
        return "GetStreamProcessorSamplesOptions{"
                + "cursorId=" + cursorId
                + ", limit=" + limit
                + ", batchSize=" + batchSize
                + '}';
    }
}
