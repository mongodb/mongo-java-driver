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

import org.bson.BsonWriter;

/**
 * The context for encoding values to BSON.
 *
 * @see org.bson.codecs.Encoder
 * @since 3.0
 */
public final class EncoderContext {

    private static final EncoderContext DEFAULT_CONTEXT = EncoderContext.builder().build();

    private final boolean encodingCollectibleDocument;

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
     * A builder for {@code EncoderContext} instances.
     */
    public static final class Builder {
        private boolean encodingCollectibleDocument;
        private Map<String, Object> context;

        private Builder() {
            context = new HashMap<String, Object>();
        }

        /**
         * Set to true if the the value to be encoded is a document that will be put in a MongoDB collection.
         *
         * @param encodingCollectibleDocument true if the value to be encoded is a document that will be put in a MongoDB collection
         * @return this
         */
        public Builder isEncodingCollectibleDocument(final boolean encodingCollectibleDocument) {
            this.encodingCollectibleDocument = encodingCollectibleDocument;
            return this;
        }

        /**
         * Method to add arbitrary encoding context information to the context
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
         * Build an instance of {@code EncoderContext}.
         *
         * @return the encoder context
         */
        public EncoderContext build() {
            return new EncoderContext(this);
        }
    }

    /**
     * Returns true if the the value to be encoded is a document that will be put in a MongoDB collection.  Encoders for such documents
     * might choose to act differently when encoding such as documents, e.g. by re-ordering the fields in some way (like encoding the _id
     * field first).
     *
     * @return true if the value to be encoded is a document that will be put in a MongoDB collection
     */
    public boolean isEncodingCollectibleDocument() {
        return encodingCollectibleDocument;
    }

    /**
     * Creates a child context based on this and serializes the value with it to the writer.
     *
     * @param encoder the encoder to encode value with
     * @param writer  the writer to encode to
     * @param value   the value to encode
     * @param <T>     the type of the value
     */
    public <T> void encodeWithChildContext(final Encoder<T> encoder, final BsonWriter writer, final T value) {
        encoder.encode(writer, value, DEFAULT_CONTEXT);
    }

    private EncoderContext(final Builder builder) {
        encodingCollectibleDocument = builder.encodingCollectibleDocument;
        context = new HashMap<String, Object>(builder.context);
    }

    /**
     * Returns the encoding context information associated with the given key, or null if no such key exists in the
     * context
     *
     * @param key
     * @return object associated with the given key or null if no such key exists
     */
    public Object getParameter(final String key) {
        return context.get(key);
    }
}
