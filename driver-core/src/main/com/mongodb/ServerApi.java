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

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;

import java.util.Objects;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A specification of the server API on which the application relies.
 *
 * @since 4.3
 */
public final class ServerApi {

    private final ServerApiVersion version;
    private final Boolean deprecationErrors;
    private final Boolean strict;

    private ServerApi(final ServerApiVersion version, @Nullable final Boolean strict, @Nullable final Boolean deprecationErrors) {
        this.version = notNull("version", version);
        this.deprecationErrors = deprecationErrors;
        this.strict = strict;
    }

    /**
     * Gets the server API version
     *
     * @return the server API version
     */
    public ServerApiVersion getVersion() {
        return version;
    }

    /**
     * Gets whether the application requires strict server API version enforcement.
     *
     * <p>
     * The default is false.
     * </p>
     *
     * @return whether the application requires strict server API version enforcement
     */
    public Optional<Boolean> getStrict() {
        return Optional.ofNullable(strict);
    }

    /**
     * Gets whether the application requires use of deprecated server APIs to be reported as errors.
     *
     * <p>
     * The default is false.
     * </p>
     *
     * @return whether the application requires use of deprecated server APIs to be reported as errors
     */
    public Optional<Boolean> getDeprecationErrors() {
        return Optional.ofNullable(deprecationErrors);
    }

    /**
     * Gets a {@code Builder} for instances of this class.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "ServerApi{"
                + "version=" + version
                + ", deprecationErrors=" + deprecationErrors
                + ", strict=" + strict
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServerApi serverApi = (ServerApi) o;

        if (version != serverApi.version) {
            return false;
        }
        if (!Objects.equals(deprecationErrors, serverApi.deprecationErrors)) {
            return false;
        }
        if (!Objects.equals(strict, serverApi.strict)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = version.hashCode();
        result = 31 * result + (deprecationErrors != null ? deprecationErrors.hashCode() : 0);
        result = 31 * result + (strict != null ? strict.hashCode() : 0);
        return result;
    }

    /**
     * A builder for {@code ServerApi} so that {@code ServerApi} can be immutable, and to support easier construction
     * through chaining.
     */
    @NotThreadSafe
    public static final class Builder {

        private ServerApiVersion version;
        private Boolean deprecationErrors;
        private Boolean strict;

        private Builder() {
        }

        /**
         * Gets the server API version
         *
         * @param version the server API version
         * @return the server API version
         */
        public Builder version(final ServerApiVersion version) {
            this.version = version;
            return this;
        }

        /**
         * Sets whether the application requires use of deprecated server APIs to be reported as errors.
         *
         * <p>
         * The default is false.
         * </p>
         *
         * @param deprecationErrors whether the application requires use of deprecated server APIs to be reported as errors
         * @return this
         */
        public Builder deprecationErrors(final boolean deprecationErrors) {
            this.deprecationErrors = deprecationErrors;
            return this;
        }

        /**
         * Sets whether the application requires strict server API version enforcement.
         *
         * <p>
         * The default is false.
         * </p>
         *
         * @param strict whether the application requires strict server API version enforcement
         * @return this
         */
        public Builder strict(final boolean strict) {
            this.strict = strict;
            return this;
        }

        /**
         * Build an instance of {@code ServerApi}.
         *
         * @return the settings from this builder
         */
        public ServerApi build() {
            return new ServerApi(version, strict, deprecationErrors);
        }
    }
}
