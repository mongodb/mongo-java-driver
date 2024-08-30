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

import com.mongodb.lang.Nullable;

import java.util.concurrent.ExecutorService;

/**
 * {@link TransportSettings} for an async transport implementation.
 *
 * @since 5.2
 */
public class AsyncTransportSettings extends TransportSettings {

    private final ExecutorService executorService;

    public AsyncTransportSettings(final Builder builder) {
        this.executorService = builder.executorService;
    }

    public static Builder builder() {
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
         * Sets the executor service
         *
         * @param executorService the executor service
         * @return this
         * @see #getExecutorService()
         */
        public Builder executorService(final ExecutorService executorService) {
            this.executorService = executorService;
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
}
