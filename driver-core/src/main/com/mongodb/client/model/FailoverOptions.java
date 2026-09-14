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
 * Options for triggering a stream processor failover.
 *
 * @since 5.5
 * @see StartStreamProcessorOptions#failover(FailoverOptions)
 */
public class FailoverOptions {
    private final String region;
    @Nullable
    private String mode;
    @Nullable
    private Boolean dryRun;

    /**
     * Constructs an instance with the required target region.
     *
     * @param region the target region for failover; must not be {@code null}
     */
    public FailoverOptions(final String region) {
        this.region = notNull("region", region);
    }

    /**
     * Gets the target region for failover.
     *
     * @return the target region
     */
    public String getRegion() {
        return region;
    }

    /**
     * Gets the failover mode.
     *
     * @return the failover mode (e.g. {@code "GRACEFUL"} or {@code "FORCED"}), or {@code null} if not set
     */
    @Nullable
    public String getMode() {
        return mode;
    }

    /**
     * Sets the failover mode. Valid values are {@code "GRACEFUL"} (default) and {@code "FORCED"}.
     *
     * @param mode the failover mode
     * @return this
     */
    public FailoverOptions mode(@Nullable final String mode) {
        this.mode = mode;
        return this;
    }

    /**
     * Gets whether this is a dry-run validation.
     *
     * @return {@code true} if this is a dry run, or {@code null} if not set
     */
    @Nullable
    public Boolean getDryRun() {
        return dryRun;
    }

    /**
     * Sets whether to validate the failover request without executing it.
     *
     * @param dryRun {@code true} to validate without executing
     * @return this
     */
    public FailoverOptions dryRun(@Nullable final Boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    @Override
    public String toString() {
        return "FailoverOptions{"
                + "region='" + region + '\''
                + ", mode='" + mode + '\''
                + ", dryRun=" + dryRun
                + '}';
    }
}
