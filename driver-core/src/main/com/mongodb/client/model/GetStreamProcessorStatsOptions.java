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
 * Options for retrieving stream processor statistics.
 *
 * @since 5.5
 */
public class GetStreamProcessorStatsOptions {
    @Nullable
    private Boolean verbose;

    /**
     * Constructs an instance with default values.
     */
    public GetStreamProcessorStatsOptions() {
    }

    /**
     * Gets whether verbose per-operator statistics are included.
     *
     * @return {@code true} if verbose statistics are requested, or {@code null} if not set
     */
    @Nullable
    public Boolean getVerbose() {
        return verbose;
    }

    /**
     * Sets whether to include per-operator statistics in the response.
     *
     * @param verbose {@code true} to include per-operator statistics
     * @return this
     */
    public GetStreamProcessorStatsOptions verbose(@Nullable final Boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    @Override
    public String toString() {
        return "GetStreamProcessorStatsOptions{"
                + "verbose=" + verbose
                + '}';
    }
}
