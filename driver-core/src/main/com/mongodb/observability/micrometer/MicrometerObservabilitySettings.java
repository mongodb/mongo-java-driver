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

package com.mongodb.observability.micrometer;

import com.mongodb.MongoConfigurationException;
import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.Reason;
import com.mongodb.lang.Nullable;
import com.mongodb.observability.ObservabilitySettings;
import io.micrometer.observation.ObservationRegistry;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The Micrometer Observation settings for tracing operations, commands and transactions.
 *
 * <p>If tracing is configured by supplying an {@code observationRegistry} then setting the environment variable
 * {@value com.mongodb.internal.observability.micrometer.TracingManager#ENV_OBSERVABILITY_ENABLED} is used to enable or disable the
 * creation of tracing spans.
 *
 * <p> If set the environment variable
 * {@value com.mongodb.internal.observability.micrometer.TracingManager#ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH}
 * will be used to determine the maximum length of command payloads captured in tracing spans.
 * If the environment variable is not set, the entire command payloads are captured (unless a {@code maxQueryTextLength} is specified via
 * the Builder).
 *
 * @since 5.7
 */
@Alpha(Reason.CLIENT)
@Immutable
public final class MicrometerObservabilitySettings extends ObservabilitySettings {

    private static final boolean OBSERVATION_REGISTRY_AVAILABLE;
    static {
        boolean isAvailable = false;
        try {
            Class.forName("io.micrometer.observation.ObservationRegistry");
            isAvailable = true;
        } catch (ClassNotFoundException e) {
            // No Micrometer support
        }
        OBSERVATION_REGISTRY_AVAILABLE = isAvailable;
    }

    @Nullable
    private final ObservationRegistry observationRegistry;
    private final int maxQueryTextLength;
    private final boolean enableCommandPayloadTracing;

    /**
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience method to create a from an existing {@code MicrometerObservabilitySettings}.
     *
     * @param settings create a builder from existing settings
     * @return a builder
     */
    public static Builder builder(final MicrometerObservabilitySettings settings) {
        return new Builder(settings);
    }

    /**
     * @return the observation registry or null
     */
    @Nullable
    public ObservationRegistry getObservationRegistry() {
        return observationRegistry;
    }

    /**
     * @return true if command payload tracing is enabled
     */
    public boolean isEnableCommandPayloadTracing() {
        return enableCommandPayloadTracing;
    }

    /**
     * @return the maximum length of command payloads captured in tracing spans.
     */
    public int getMaxQueryTextLength() {
        return maxQueryTextLength;
    }

    /**
     * A builder for {@code MicrometerObservabilitySettings}
     */
    @NotThreadSafe
    public static final class Builder {
        @Nullable
        private ObservationRegistry observationRegistry;
        private boolean enableCommandPayloadTracing;
        private int maxQueryTextLength = Integer.MAX_VALUE;

        private Builder() {
            if (!OBSERVATION_REGISTRY_AVAILABLE) {
                throw new MongoConfigurationException("The 'io.micrometer.observation' dependency is required for "
                        + "MicrometerObservabilitySettings.");
            }
        }
        private Builder(final MicrometerObservabilitySettings settings) {
            this.observationRegistry = settings.observationRegistry;
            this.enableCommandPayloadTracing = settings.enableCommandPayloadTracing;
            this.maxQueryTextLength = settings.maxQueryTextLength;
        }

        /**
         * Applies the MicrometerObservabilitySettings to the builder
         *
         * <p>Note: Overwrites all existing settings</p>
         *
         * @param settings the MicrometerObservabilitySettings
         * @return this
         */
        public MicrometerObservabilitySettings.Builder applySettings(final MicrometerObservabilitySettings settings) {
            notNull("settings", settings);
            observationRegistry = settings.observationRegistry;
            enableCommandPayloadTracing = settings.enableCommandPayloadTracing;
            maxQueryTextLength = settings.maxQueryTextLength;
            return this;
        }

        /**
         * Sets the observation registry to use for creating tracing Spans for operations, commands and transactions.
         *
         * @param observationRegistry the observation registry
         * @return this
         * @since 5.7
         */
        @Alpha(Reason.CLIENT)
        public Builder observationRegistry(@Nullable final ObservationRegistry observationRegistry) {
            this.observationRegistry = observationRegistry;
            return this;
        }

        /**
         * Sets the observation registry to use for creating tracing Spans for operations, commands and transactions.
         *
         * @param enableCommandPayload whether command payloads should be captured in tracing spans. This may have performance
         *                             implications so should be used with care.
         * @return this
         * @since 5.7
         */
        @Alpha(Reason.CLIENT)
        public Builder enableCommandPayloadTracing(final boolean enableCommandPayload) {
            this.enableCommandPayloadTracing = enableCommandPayload;
            return this;
        }

        /**
         * Sets the maximum length of command payloads captured in tracing spans. If not set, the entire command payload is captured.
         *
         * @param maxQueryTextLength the maximum length of command payloads captured in tracing spans.
         * @return this
         * @since 5.7
         */
        @Alpha(Reason.CLIENT)
        public Builder maxQueryTextLength(final int maxQueryTextLength) {
            this.maxQueryTextLength = maxQueryTextLength;
            return this;
        }

        /**
         * @return the configured settings
         */
        public MicrometerObservabilitySettings build() {
            return new MicrometerObservabilitySettings(observationRegistry, enableCommandPayloadTracing, maxQueryTextLength);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MicrometerObservabilitySettings that = (MicrometerObservabilitySettings) o;
        return enableCommandPayloadTracing == that.enableCommandPayloadTracing
                && Objects.equals(observationRegistry, that.observationRegistry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(observationRegistry, enableCommandPayloadTracing);
    }

    private MicrometerObservabilitySettings(@Nullable final ObservationRegistry observationRegistry,
            final boolean enableCommandPayloadTracing, final int maxQueryTextLength) {
        this.observationRegistry = observationRegistry;
        this.enableCommandPayloadTracing = enableCommandPayloadTracing;
        this.maxQueryTextLength = maxQueryTextLength;
    }
}
