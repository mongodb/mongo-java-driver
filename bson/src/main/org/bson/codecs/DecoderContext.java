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

package org.bson.codecs;

import org.bson.BsonReader;

import java.util.HashMap;
import java.util.Map;

import static org.bson.assertions.Assertions.notNull;

/**
 * The context for decoding values to BSON.
 *
 * @see org.bson.codecs.Decoder
 * @since 3.0
 */
public final class DecoderContext {
    private static final DecoderContext DEFAULT_CONTEXT = DecoderContext.builder().build();
    private final boolean checkedDiscriminator;
    private final Map<String, Object> attachments;

    /**
     * Attaches an item to this context with the given name.  Returns a new modified context.
     *
     * @param key the key for the attachment
     * @param object the item to attach
     * @return the new DecoderContext
     */
    public DecoderContext attach(String key, Object object) {
        return builder()
                .attachments(attachments)
                .checkedDiscriminator(checkedDiscriminator)
                .attach(key, object)
                .build();
    }
    /**
     * @return true if the discriminator has been checked
     */
    public boolean hasCheckedDiscriminator() {
        return checkedDiscriminator;
    }

    /**
     * Fetches the named attachment.  Will return null if that name doesn't have an attachment.
     *
     * @param key the name of the attachment
     * @param <T> the type of the attachment
     * @return the attachment
     */
    @SuppressWarnings("unchecked")
    public <T> T attachment(String key) {
        return (T) attachments.get(key);
    }
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
        private Builder() {
        }

        private final Map<String, Object> attachments = new HashMap<>();
        private boolean checkedDiscriminator;

        public Map<String, Object> attachments() {
            return attachments;
        }

        public Builder attach(String key, Object attachment) {
            this.attachments.put(key, attachment);
            return this;
        }

        public Builder attachments(Map<String, Object> attachments) {
            this.attachments.putAll(attachments);
            return this;
        }

        /**
         * @return true if the discriminator has been checked
         */
        public boolean hasCheckedDiscriminator() {
            return checkedDiscriminator;
        }

        /**
         * Sets the checkedDiscriminator
         *
         * @param checkedDiscriminator the checkedDiscriminator
         * @return this
         */
        public Builder checkedDiscriminator(final boolean checkedDiscriminator) {
            this.checkedDiscriminator = checkedDiscriminator;
            return this;
        }

        /**
         * Build an instance of {@code DecoderContext}.
         * @return the decoder context
         */
        public DecoderContext build() {
            return new DecoderContext(this);
        }
    }

    /**
     * Creates a child context and then deserializes using the reader.
     *
     * @param decoder the decoder to decode with
     * @param reader the reader to decode to
     * @param <T> the type of the decoder
     * @return the decoded value
     * @since 3.5
     */
    public <T> T decodeWithChildContext(final Decoder<T> decoder, final BsonReader reader) {
        notNull("decoder", decoder);
        return decoder.decode(reader, DEFAULT_CONTEXT);
    }

    private DecoderContext(final Builder builder) {
        this.attachments = builder.attachments();
        this.checkedDiscriminator = builder.hasCheckedDiscriminator();
    }
}
