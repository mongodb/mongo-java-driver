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

package com.mongodb.internal;

import com.mongodb.lang.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Centralized access to environment variables. All production code should use
 * this class instead of calling {@link System#getenv(String)} directly.
 *
 * <p>Tests can override values via {@link #envOverride()}.</p>
 */
public final class EnvironmentProvider {
    private static UnaryOperator<String> envLookup = System::getenv;

    private EnvironmentProvider() {
    }

    @Nullable
    public static String getEnv(final String key) {
        return envLookup.apply(key);
    }

    /** Exists only for testing **/
    public static EnvironmentOverride envOverride() {
        return new EnvironmentOverride();
    }

    public static final class EnvironmentOverride implements AutoCloseable {
        private final Map<String, String> overrides = new HashMap<>();
        private final UnaryOperator<String> original;

        private EnvironmentOverride() {
            original = envLookup;
            envLookup = key -> overrides.containsKey(key)
                    ? overrides.get(key)
                    : original.apply(key);
        }

        public EnvironmentOverride set(final String key, @Nullable final String value) {
            overrides.put(key, value);
            return this;
        }

        @Override
        public void close() {
            envLookup = original;
        }
    }
}