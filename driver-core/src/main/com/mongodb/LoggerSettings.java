/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.annotations.Immutable;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An immutable class representing settings for logging.
 *
 * <p>
 * The driver logs using SLF4J 1.0 API with a root logger of {@code org.mongodb.driver}.
 * </p>
 *
 * @since 4.9
 */
@Immutable
public final class LoggerSettings {
    private final int maxCommandLoggingDocumentLength;
    /**
     * Gets a builder for an instance of {@code LoggerSettings}.
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder instance.
     *
     * @param loggerSettings existing LoggerSettings to default the builder settings on.
     * @return a builder
     */
    public static Builder builder(final LoggerSettings loggerSettings) {
        return builder().applySettings(loggerSettings);
    }

    /**
     * A builder for an instance of {@code LoggerSettings}.
     */
    public static final class Builder {
        private int maxCommandLoggingDocumentLength = 1000;
        private Builder() {
        }

        /**
         * Applies the loggerSettings to the builder
         *
         * <p>Note: Overwrites all existing settings</p>
         *
         * @param loggerSettings the loggerSettings
         * @return this
         */
        public Builder applySettings(final LoggerSettings loggerSettings) {
            notNull("loggerSettings", loggerSettings);
            maxCommandLoggingDocumentLength = loggerSettings.maxCommandLoggingDocumentLength;
            return this;
        }

        /**
         * Sets the max command logging document length.
         *
         * @param maxCommandLoggingDocumentLength the max command logging document length
         * @return this
         */
        public Builder maxCommandLoggingDocumentLength(final int maxCommandLoggingDocumentLength) {
            this.maxCommandLoggingDocumentLength = maxCommandLoggingDocumentLength;
            return this;
        }

        /**
         * Build an instance of {@code LoggerSettings}.
         * @return the logger settings for this builder
         */
        public LoggerSettings build() {
            return new LoggerSettings(this);
        }
    }

    /**
     * Gets the max command logging document length.
     *
     * <p>
     * When the driver logs a command and its reply via the {@code org.mongodb.driver.protocol.command} SFL4J logger, it truncates the
     * them to the maximum length defined by this setting.
     * </p>
     *
     * <p>
     * Defaults to 1000 characters.
     * </p>
     *
     * @return the max command logging document length
     */
    public int getMaxCommandLoggingDocumentLength() {
        return maxCommandLoggingDocumentLength;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoggerSettings that = (LoggerSettings) o;
        return maxCommandLoggingDocumentLength == that.maxCommandLoggingDocumentLength;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxCommandLoggingDocumentLength);
    }

    @Override
    public String toString() {
        return "LoggerSettings{"
                + "maxCommandLoggingDocumentLength=" + maxCommandLoggingDocumentLength
                + '}';
    }

    private LoggerSettings(final Builder builder) {
        maxCommandLoggingDocumentLength = builder.maxCommandLoggingDocumentLength;
    }
}
