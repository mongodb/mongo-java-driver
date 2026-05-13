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
import org.bson.conversions.Bson;

/**
 * Options for creating a stream processor.
 *
 * @since 5.5
 * @see com.mongodb.client.StreamProcessors#create(String, java.util.List, CreateStreamProcessorOptions)
 */
public class CreateStreamProcessorOptions {
    @Nullable
    private Bson dlq;
    @Nullable
    private String streamMetaFieldName;
    @Nullable
    private String tier;
    @Nullable
    private Boolean failover;

    /**
     * Constructs an instance with default values.
     */
    public CreateStreamProcessorOptions() {
    }

    /**
     * Gets the dead letter queue configuration document.
     *
     * @return the DLQ configuration, or {@code null} if not set
     */
    @Nullable
    public Bson getDlq() {
        return dlq;
    }

    /**
     * Sets the dead letter queue configuration document.
     *
     * @param dlq the DLQ configuration
     * @return this
     */
    public CreateStreamProcessorOptions dlq(@Nullable final Bson dlq) {
        this.dlq = dlq;
        return this;
    }

    /**
     * Gets the field name used for stream metadata.
     *
     * @return the stream meta field name, or {@code null} if not set
     */
    @Nullable
    public String getStreamMetaFieldName() {
        return streamMetaFieldName;
    }

    /**
     * Sets the field name used for stream metadata.
     *
     * @param streamMetaFieldName the stream meta field name
     * @return this
     */
    public CreateStreamProcessorOptions streamMetaFieldName(@Nullable final String streamMetaFieldName) {
        this.streamMetaFieldName = streamMetaFieldName;
        return this;
    }

    /**
     * Gets the compute tier for the stream processor.
     *
     * @return the tier, or {@code null} if not set
     */
    @Nullable
    public String getTier() {
        return tier;
    }

    /**
     * Sets the compute tier for the stream processor (e.g. {@code "SP10"}).
     *
     * @param tier the compute tier
     * @return this
     */
    public CreateStreamProcessorOptions tier(@Nullable final String tier) {
        this.tier = tier;
        return this;
    }

    /**
     * Gets whether failover is enabled for the stream processor.
     *
     * @return {@code true} if failover is enabled, or {@code null} if not set
     */
    @Nullable
    public Boolean getFailover() {
        return failover;
    }

    /**
     * Sets whether failover is enabled for the stream processor.
     *
     * @param failover {@code true} to enable failover
     * @return this
     */
    public CreateStreamProcessorOptions failover(@Nullable final Boolean failover) {
        this.failover = failover;
        return this;
    }

    @Override
    public String toString() {
        return "CreateStreamProcessorOptions{"
                + "dlq=" + dlq
                + ", streamMetaFieldName='" + streamMetaFieldName + '\''
                + ", tier='" + tier + '\''
                + ", failover=" + failover
                + '}';
    }
}
