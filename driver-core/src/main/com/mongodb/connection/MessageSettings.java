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

package com.mongodb.connection;

import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;

/**
 * The message settings
 *
 * @since 3.0
 */
@Immutable
final class MessageSettings {
    private static final int DEFAULT_MAX_DOCUMENT_SIZE = 0x1000000;  // 16MB
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 0x2000000;   // 32MB
    private static final int DEFAULT_MAX_BATCH_COUNT = 1000;

    private final int maxDocumentSize;
    private final int maxMessageSize;
    private final int maxBatchCount;
    private final ServerVersion serverVersion;
    private final ServerType serverType;

    /**
     * Gets the builder
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A MessageSettings builder.
     */
    @NotThreadSafe
    public static final class Builder {
        private int maxDocumentSize = DEFAULT_MAX_DOCUMENT_SIZE;
        private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        private int maxBatchCount = DEFAULT_MAX_BATCH_COUNT;
        private ServerVersion serverVersion;
        private ServerType serverType;

        /**
         * Build it.
         *
         * @return the message settings
         */
        public MessageSettings build() {
            return new MessageSettings(this);
        }

        /**
         * Sets the maximum document size allowed.
         *
         * @param maxDocumentSize the maximum document size allowed
         * @return this
         */
        public Builder maxDocumentSize(final int maxDocumentSize) {
            this.maxDocumentSize = maxDocumentSize;
            return this;
        }

        /**
         * Sets the maximum message size allowed.
         *
         * @param maxMessageSize the maximum message size allowed
         * @return this
         */
        public Builder maxMessageSize(final int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        /**
         * Sets the maximum number of items in a batch allowed.
         *
         * @param maxBatchCount the maximum number of items in a batch allowed
         * @return this
         */
        public Builder maxBatchCount(final int maxBatchCount) {
            this.maxBatchCount = maxBatchCount;
            return this;
        }

        public Builder serverVersion(final ServerVersion serverVersion) {
            this.serverVersion = serverVersion;
            return this;
        }

        public Builder serverType(final ServerType serverType) {
            this.serverType = serverType;
            return this;
        }
    }

    /**
     * Gets the maximum document size allowed.
     *
     * @return the maximum document size allowed
     */
    public int getMaxDocumentSize() {
        return maxDocumentSize;
    }

    /**
     * Gets the maximum message size allowed.
     *
     * @return the maximum message size allowed
     */
    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    /**
     * Gets the maximum number of items in a batch allowed.
     *
     * @return the maximum number of items in a batch allowed
     */
    public int getMaxBatchCount() {
        return maxBatchCount;
    }

    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    public ServerType getServerType() {
        return serverType;
    }

    private MessageSettings(final Builder builder) {
        this.maxDocumentSize = builder.maxDocumentSize;
        this.maxMessageSize = builder.maxMessageSize;
        this.maxBatchCount = builder.maxBatchCount;
        this.serverVersion = builder.serverVersion;
        this.serverType = builder.serverType;
    }
}
