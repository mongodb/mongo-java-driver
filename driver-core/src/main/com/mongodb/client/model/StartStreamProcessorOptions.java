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
import org.bson.BsonTimestamp;

/**
 * Options for starting a stream processor.
 *
 * @since 5.5
 */
public class StartStreamProcessorOptions {
    @Nullable
    private Integer workers;
    @Nullable
    private Boolean clearCheckpoints;
    @Nullable
    private BsonTimestamp startAtOperationTime;
    @Nullable
    private String tier;
    @Nullable
    private Boolean enableAutoScaling;
    @Nullable
    private FailoverOptions failover;

    /**
     * Constructs an instance with default values.
     */
    public StartStreamProcessorOptions() {
    }

    /**
     * Gets the number of workers.
     *
     * @return the number of workers, or {@code null} if not set
     */
    @Nullable
    public Integer getWorkers() {
        return workers;
    }

    /**
     * Sets the number of workers for the stream processor.
     *
     * @param workers the number of workers
     * @return this
     */
    public StartStreamProcessorOptions workers(@Nullable final Integer workers) {
        this.workers = workers;
        return this;
    }

    /**
     * Gets whether checkpoints should be cleared on start.
     *
     * @return {@code true} if checkpoints should be cleared, or {@code null} if not set
     */
    @Nullable
    public Boolean getClearCheckpoints() {
        return clearCheckpoints;
    }

    /**
     * Sets whether to clear checkpoints when starting the processor.
     *
     * @param clearCheckpoints {@code true} to clear checkpoints
     * @return this
     */
    public StartStreamProcessorOptions clearCheckpoints(@Nullable final Boolean clearCheckpoints) {
        this.clearCheckpoints = clearCheckpoints;
        return this;
    }

    /**
     * Gets the operation time from which to start processing.
     *
     * @return the start operation time, or {@code null} if not set
     */
    @Nullable
    public BsonTimestamp getStartAtOperationTime() {
        return startAtOperationTime;
    }

    /**
     * Sets the operation time from which to resume processing.
     *
     * @param startAtOperationTime the operation time
     * @return this
     */
    public StartStreamProcessorOptions startAtOperationTime(@Nullable final BsonTimestamp startAtOperationTime) {
        this.startAtOperationTime = startAtOperationTime;
        return this;
    }

    /**
     * Gets the compute tier.
     *
     * @return the tier, or {@code null} if not set
     */
    @Nullable
    public String getTier() {
        return tier;
    }

    /**
     * Sets the compute tier. Valid values: {@code "SP2"}, {@code "SP5"}, {@code "SP10"}, {@code "SP30"}, {@code "SP50"}.
     *
     * @param tier the compute tier
     * @return this
     */
    public StartStreamProcessorOptions tier(@Nullable final String tier) {
        this.tier = tier;
        return this;
    }

    /**
     * Gets whether auto-scaling is enabled.
     *
     * @return {@code true} if auto-scaling is enabled, or {@code null} if not set
     */
    @Nullable
    public Boolean getEnableAutoScaling() {
        return enableAutoScaling;
    }

    /**
     * Sets whether to enable auto-scaling.
     *
     * @param enableAutoScaling {@code true} to enable auto-scaling
     * @return this
     */
    public StartStreamProcessorOptions enableAutoScaling(@Nullable final Boolean enableAutoScaling) {
        this.enableAutoScaling = enableAutoScaling;
        return this;
    }

    /**
     * Gets the failover options.
     *
     * @return the failover options, or {@code null} if not set
     */
    @Nullable
    public FailoverOptions getFailover() {
        return failover;
    }

    /**
     * Sets the failover options.
     *
     * @param failover the failover options
     * @return this
     */
    public StartStreamProcessorOptions failover(@Nullable final FailoverOptions failover) {
        this.failover = failover;
        return this;
    }

    @Override
    public String toString() {
        return "StartStreamProcessorOptions{"
                + "workers=" + workers
                + ", clearCheckpoints=" + clearCheckpoints
                + ", startAtOperationTime=" + startAtOperationTime
                + ", tier='" + tier + '\''
                + ", enableAutoScaling=" + enableAutoScaling
                + ", failover=" + failover
                + '}';
    }
}
