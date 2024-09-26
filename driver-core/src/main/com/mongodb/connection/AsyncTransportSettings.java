/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package com.mongodb.connection;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;

import java.nio.channels.AsynchronousChannelGroup;
import java.util.concurrent.ExecutorService;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * {@link TransportSettings} for a non-<a href="http://netty.io/">Netty</a>-based async transport implementation.
 *
 * @since 5.2
 */
@Immutable
public final class AsyncTransportSettings extends TransportSettings {

    private final ExecutorService executorService;

    private AsyncTransportSettings(final Builder builder) {
        this.executorService = builder.executorService;
    }

    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for an instance of {@link AsyncTransportSettings}
     */
    public static final class Builder {

        private ExecutorService executorService;

        private Builder() {
        }

        /**
         * Sets the executor service. This executor service will not be shut
         * down by the driver code, and must be shut down by application code.
         *
         * @param executorService the executor service
         * @return this
         * @see #getExecutorService()
         * @see AsynchronousChannelGroup#withThreadPool(ExecutorService)
         */
        public Builder executorService(final ExecutorService executorService) {
            this.executorService = notNull("executorService", executorService);
            return this;
        }

        /**
         * Build an instance of {@link AsyncTransportSettings}
         * @return an instance of {@link AsyncTransportSettings}
         */
        public AsyncTransportSettings build() {
            return new AsyncTransportSettings(this);
        }
    }

    /**
     * Gets the executor service
     *
     * @return the executor service
     * @see Builder#executorService(ExecutorService)
     */
    @Nullable
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    public String toString() {
        return "AsyncTransportSettings{" +
                "executorService=" + executorService +
                '}';
    }
}
