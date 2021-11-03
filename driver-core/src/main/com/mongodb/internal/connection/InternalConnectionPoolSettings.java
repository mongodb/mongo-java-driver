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
package com.mongodb.internal.connection;

import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import java.util.Objects;

@Immutable
public final class InternalConnectionPoolSettings {
    private final boolean prestartAsyncWorkManager;

    private InternalConnectionPoolSettings(final Builder builder) {
        prestartAsyncWorkManager = builder.prestartAsyncWorkManager;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Specifies whether to pre-start the asynchronous work manager of the pool.
     * <p>
     * Default is {@code false}.
     *
     * @return {@code true} iff pool's asynchronous work manager must be pre-started.
     * @see Builder#prestartAsyncWorkManager(boolean)
     */
    public boolean isPrestartAsyncWorkManager() {
        return prestartAsyncWorkManager;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final InternalConnectionPoolSettings that = (InternalConnectionPoolSettings) o;
        return prestartAsyncWorkManager == that.prestartAsyncWorkManager;
    }

    @Override
    public int hashCode() {
        return Objects.hash(prestartAsyncWorkManager);
    }

    @Override
    public String toString() {
        return "InternalConnectionPoolSettings{"
                + "prestartAsyncWorkManager=" + prestartAsyncWorkManager
                + '}';
    }

    @NotThreadSafe
    public static final class Builder {
        private boolean prestartAsyncWorkManager = false;

        private Builder() {
        }

        /**
         * Allows to pre-start the asynchronous work manager of the pool.
         *
         * @param prestart {@code true} iff pool's asynchronous work manager must be pre-started.
         * @return {@code this}.
         * @see InternalConnectionPoolSettings#isPrestartAsyncWorkManager()
         */
        public Builder prestartAsyncWorkManager(final boolean prestart) {
            prestartAsyncWorkManager = prestart;
            return this;
        }

        public InternalConnectionPoolSettings build() {
            return new InternalConnectionPoolSettings(this);
        }
    }
}
