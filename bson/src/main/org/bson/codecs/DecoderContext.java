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

package org.bson.codecs;

import java.util.HashMap;
import java.util.Map;

/**
 * The context for decoding values to BSON. Currently this is a placeholder, as there is nothing needed yet.
 *
 * @see org.bson.codecs.Decoder
 * @since 3.0
 */
public final class DecoderContext {
    private Map<String, Object> context;

    /**
     * Create a builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@code DecoderContext} instances.
     */
    public static final class Builder {
        private Map<String, Object> context;

        private Builder() {
            context = new HashMap<String, Object>();
        }

        /**
         * Method to add arbitrary decoding context information to the context
         *
         * @param key   identifier for the configuration
         * @param value configuration value
         * @return
         */
        public Builder addParameter(final String key, final Object value) {
            context.put(key, value);
            return this;
        }

        /**
         * Build an instance of {@code DecoderContext}.
         *
         * @return the decoder context
         */
        public DecoderContext build() {
            return new DecoderContext(this);
        }
    }

    private DecoderContext(final Builder builder) {
        context = new HashMap<String, Object>(builder.context);
    }

    /**
     * Returns the decoding context information associated with the given key, or null if no such key exists in the
     * context
     *
     * @param key
     * @return object associated with the given key or null if no such key exists
     */
    public Object getParameter(final String key) {
        return context.get(key);
    }
}
