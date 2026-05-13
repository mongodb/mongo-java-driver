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
import org.bson.BsonDocument;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Information about a stream processor, as returned by {@code getStreamProcessor}.
 *
 * <p>The {@code state} field is returned as a raw string. Known values include
 * {@code "CREATING"}, {@code "VALIDATING"}, {@code "CREATED"}, {@code "PROVISIONING"},
 * {@code "STARTED"}, {@code "STOPPING"}, {@code "STOPPED"}, {@code "DROPPED"}, and
 * {@code "FAILED"}, but additional states may be introduced in future server versions.</p>
 *
 * @since 5.5
 */
public final class StreamProcessorInfo {
    private final String id;
    private final String name;
    private final String state;
    private final List<BsonDocument> pipeline;
    private final int pipelineVersion;
    @Nullable
    private final String tier;
    @Nullable
    private final BsonDocument dlq;
    @Nullable
    private final String streamMetaFieldName;
    private final boolean enableAutoScaling;
    private final boolean failoverEnabled;
    private final String activeRegion;
    private final String workspaceDefaultRegion;
    @Nullable
    private final Date lastStateChange;
    @Nullable
    private final Date lastModifiedAt;
    private final String modifiedBy;
    private final boolean hasStarted;
    private final String errorMsg;
    private final boolean errorRetryable;
    @Nullable
    private final Integer errorCode;

    private StreamProcessorInfo(final Builder builder) {
        this.id = notNull("id", builder.id);
        this.name = notNull("name", builder.name);
        this.state = notNull("state", builder.state);
        this.pipeline = Collections.unmodifiableList(notNull("pipeline", builder.pipeline));
        this.pipelineVersion = builder.pipelineVersion;
        this.tier = builder.tier;
        this.dlq = builder.dlq;
        this.streamMetaFieldName = builder.streamMetaFieldName;
        this.enableAutoScaling = builder.enableAutoScaling;
        this.failoverEnabled = builder.failoverEnabled;
        this.activeRegion = notNull("activeRegion", builder.activeRegion);
        this.workspaceDefaultRegion = notNull("workspaceDefaultRegion", builder.workspaceDefaultRegion);
        this.lastStateChange = builder.lastStateChange;
        this.lastModifiedAt = builder.lastModifiedAt;
        this.modifiedBy = notNull("modifiedBy", builder.modifiedBy);
        this.hasStarted = builder.hasStarted;
        this.errorMsg = notNull("errorMsg", builder.errorMsg);
        this.errorRetryable = builder.errorRetryable;
        this.errorCode = builder.errorCode;
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the processor ID.
     *
     * @return the processor ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the processor name.
     *
     * @return the processor name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the current state of the processor.
     *
     * <p>The value is returned as-is from the server. Known values include {@code "CREATING"},
     * {@code "VALIDATING"}, {@code "CREATED"}, {@code "PROVISIONING"}, {@code "STARTED"},
     * {@code "STOPPING"}, {@code "STOPPED"}, {@code "DROPPED"}, and {@code "FAILED"},
     * but additional states may be introduced in future server versions.</p>
     *
     * @return the processor state
     */
    public String getState() {
        return state;
    }

    /**
     * Gets the processor pipeline.
     *
     * @return an unmodifiable list of pipeline stage documents
     */
    public List<BsonDocument> getPipeline() {
        return pipeline;
    }

    /**
     * Gets the pipeline version, incremented on each successful modification.
     *
     * @return the pipeline version
     */
    public int getPipelineVersion() {
        return pipelineVersion;
    }

    /**
     * Gets the compute tier.
     *
     * @return the tier (e.g. {@code "SP10"}), or {@code null} if not set
     */
    @Nullable
    public String getTier() {
        return tier;
    }

    /**
     * Gets the dead letter queue configuration.
     *
     * @return the DLQ configuration document, or {@code null} if not configured
     */
    @Nullable
    public BsonDocument getDlq() {
        return dlq;
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
     * Gets whether auto-scaling is enabled.
     *
     * @return {@code true} if auto-scaling is enabled
     */
    public boolean isEnableAutoScaling() {
        return enableAutoScaling;
    }

    /**
     * Gets whether failover is enabled.
     *
     * @return {@code true} if failover is enabled
     */
    public boolean isFailoverEnabled() {
        return failoverEnabled;
    }

    /**
     * Gets the region where the processor is currently deployed.
     *
     * @return the active region
     */
    public String getActiveRegion() {
        return activeRegion;
    }

    /**
     * Gets the workspace's default region.
     *
     * <p>This may differ from {@link #getActiveRegion()} during or after a failover.</p>
     *
     * @return the workspace default region
     */
    public String getWorkspaceDefaultRegion() {
        return workspaceDefaultRegion;
    }

    /**
     * Gets the time of the last state change.
     *
     * @return the last state change time, or {@code null} if not available
     */
    @Nullable
    public Date getLastStateChange() {
        return lastStateChange;
    }

    /**
     * Gets the time the processor was last modified.
     *
     * @return the last modified time, or {@code null} if not available
     */
    @Nullable
    public Date getLastModifiedAt() {
        return lastModifiedAt;
    }

    /**
     * Gets the identity of the user who last modified the processor.
     *
     * @return the modifier's identity
     */
    public String getModifiedBy() {
        return modifiedBy;
    }

    /**
     * Gets whether the processor has ever been started.
     *
     * @return {@code true} if the processor has been started at least once
     */
    public boolean isHasStarted() {
        return hasStarted;
    }

    /**
     * Gets the current error message.
     *
     * <p>Returns an empty string when no error has occurred.</p>
     *
     * @return the error message
     */
    public String getErrorMsg() {
        return errorMsg;
    }

    /**
     * Gets whether the current error is retryable.
     *
     * @return {@code true} if the error is retryable
     */
    public boolean isErrorRetryable() {
        return errorRetryable;
    }

    /**
     * Gets the error code.
     *
     * @return the error code, or {@code null} if no error has occurred
     */
    @Nullable
    public Integer getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return "StreamProcessorInfo{"
                + "id='" + id + '\''
                + ", name='" + name + '\''
                + ", state='" + state + '\''
                + ", pipelineVersion=" + pipelineVersion
                + ", tier='" + tier + '\''
                + ", enableAutoScaling=" + enableAutoScaling
                + ", failoverEnabled=" + failoverEnabled
                + ", activeRegion='" + activeRegion + '\''
                + ", workspaceDefaultRegion='" + workspaceDefaultRegion + '\''
                + ", modifiedBy='" + modifiedBy + '\''
                + ", hasStarted=" + hasStarted
                + ", errorMsg='" + errorMsg + '\''
                + ", errorRetryable=" + errorRetryable
                + ", errorCode=" + errorCode
                + '}';
    }

    /**
     * A builder for {@link StreamProcessorInfo}.
     */
    public static final class Builder {
        @Nullable
        private String id;
        @Nullable
        private String name;
        @Nullable
        private String state;
        @Nullable
        private List<BsonDocument> pipeline;
        private int pipelineVersion;
        @Nullable
        private String tier;
        @Nullable
        private BsonDocument dlq;
        @Nullable
        private String streamMetaFieldName;
        private boolean enableAutoScaling;
        private boolean failoverEnabled;
        @Nullable
        private String activeRegion;
        @Nullable
        private String workspaceDefaultRegion;
        @Nullable
        private Date lastStateChange;
        @Nullable
        private Date lastModifiedAt;
        @Nullable
        private String modifiedBy;
        private boolean hasStarted;
        @Nullable
        private String errorMsg;
        private boolean errorRetryable;
        @Nullable
        private Integer errorCode;

        private Builder() {
        }

        /**
         * Sets the processor ID.
         *
         * @param id the processor ID
         * @return this
         */
        public Builder id(final String id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the processor name.
         *
         * @param name the processor name
         * @return this
         */
        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the processor state.
         *
         * @param state the processor state
         * @return this
         */
        public Builder state(final String state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the processor pipeline.
         *
         * @param pipeline the pipeline stage documents
         * @return this
         */
        public Builder pipeline(final List<BsonDocument> pipeline) {
            this.pipeline = pipeline;
            return this;
        }

        /**
         * Sets the pipeline version.
         *
         * @param pipelineVersion the pipeline version
         * @return this
         */
        public Builder pipelineVersion(final int pipelineVersion) {
            this.pipelineVersion = pipelineVersion;
            return this;
        }

        /**
         * Sets the compute tier.
         *
         * @param tier the compute tier
         * @return this
         */
        public Builder tier(@Nullable final String tier) {
            this.tier = tier;
            return this;
        }

        /**
         * Sets the dead letter queue configuration.
         *
         * @param dlq the DLQ configuration document
         * @return this
         */
        public Builder dlq(@Nullable final BsonDocument dlq) {
            this.dlq = dlq;
            return this;
        }

        /**
         * Sets the stream meta field name.
         *
         * @param streamMetaFieldName the stream meta field name
         * @return this
         */
        public Builder streamMetaFieldName(@Nullable final String streamMetaFieldName) {
            this.streamMetaFieldName = streamMetaFieldName;
            return this;
        }

        /**
         * Sets whether auto-scaling is enabled.
         *
         * @param enableAutoScaling {@code true} if auto-scaling is enabled
         * @return this
         */
        public Builder enableAutoScaling(final boolean enableAutoScaling) {
            this.enableAutoScaling = enableAutoScaling;
            return this;
        }

        /**
         * Sets whether failover is enabled.
         *
         * @param failoverEnabled {@code true} if failover is enabled
         * @return this
         */
        public Builder failoverEnabled(final boolean failoverEnabled) {
            this.failoverEnabled = failoverEnabled;
            return this;
        }

        /**
         * Sets the active region.
         *
         * @param activeRegion the active region
         * @return this
         */
        public Builder activeRegion(final String activeRegion) {
            this.activeRegion = activeRegion;
            return this;
        }

        /**
         * Sets the workspace default region.
         *
         * @param workspaceDefaultRegion the workspace default region
         * @return this
         */
        public Builder workspaceDefaultRegion(final String workspaceDefaultRegion) {
            this.workspaceDefaultRegion = workspaceDefaultRegion;
            return this;
        }

        /**
         * Sets the last state change time.
         *
         * @param lastStateChange the last state change time
         * @return this
         */
        public Builder lastStateChange(@Nullable final Date lastStateChange) {
            this.lastStateChange = lastStateChange;
            return this;
        }

        /**
         * Sets the last modified time.
         *
         * @param lastModifiedAt the last modified time
         * @return this
         */
        public Builder lastModifiedAt(@Nullable final Date lastModifiedAt) {
            this.lastModifiedAt = lastModifiedAt;
            return this;
        }

        /**
         * Sets the identity of the user who last modified the processor.
         *
         * @param modifiedBy the modifier's identity
         * @return this
         */
        public Builder modifiedBy(final String modifiedBy) {
            this.modifiedBy = modifiedBy;
            return this;
        }

        /**
         * Sets whether the processor has ever been started.
         *
         * @param hasStarted {@code true} if the processor has been started
         * @return this
         */
        public Builder hasStarted(final boolean hasStarted) {
            this.hasStarted = hasStarted;
            return this;
        }

        /**
         * Sets the error message.
         *
         * @param errorMsg the error message; empty string when no error
         * @return this
         */
        public Builder errorMsg(final String errorMsg) {
            this.errorMsg = errorMsg;
            return this;
        }

        /**
         * Sets whether the current error is retryable.
         *
         * @param errorRetryable {@code true} if the error is retryable
         * @return this
         */
        public Builder errorRetryable(final boolean errorRetryable) {
            this.errorRetryable = errorRetryable;
            return this;
        }

        /**
         * Sets the error code.
         *
         * @param errorCode the error code, or {@code null} if no error
         * @return this
         */
        public Builder errorCode(@Nullable final Integer errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        /**
         * Builds a {@link StreamProcessorInfo} from the current state of this builder.
         *
         * @return the built instance
         */
        public StreamProcessorInfo build() {
            return new StreamProcessorInfo(this);
        }
    }
}
