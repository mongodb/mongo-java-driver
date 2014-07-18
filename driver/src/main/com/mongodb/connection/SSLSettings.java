/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

public class SSLSettings {
    private final boolean enabled;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean enabled;

        // CHECKSTYLE:OFF
        public Builder enabled(final boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        // CHECKSTYLE:ON

        public SSLSettings build() {
            return new SSLSettings(this);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public SSLSettings(final Builder builder) {
        enabled = builder.enabled;
    }

    @Override
    public String toString() {
        return "SSLSettings{"
               + "enabled=" + enabled
               + '}';
    }
}
