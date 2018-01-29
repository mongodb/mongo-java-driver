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
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.configuration.CodecRegistry;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A codec for unknown BsonValues.
 *
 * <p>Useful for decoding a mix of differing Bson types.</p>
 *
 * @since 3.0
 */
public class BsonValueCodec implements Codec<BsonValue> {

    private final CodecRegistry codecRegistry;

    /**
     * Creates a new instance with a default codec registry that uses the {@link BsonValueCodecProvider}.
     */
    public BsonValueCodec() {
        this(fromProviders(new BsonValueCodecProvider()));
    }

    /**
     * Creates a new instance initialised with the given codec registry.
     *
     * @param codecRegistry the {@code CodecRegistry} to use to look up the codecs for encoding and decoding to/from BSON
     */
    public BsonValueCodec(final CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    @Override
    public BsonValue decode(final BsonReader reader, final DecoderContext decoderContext) {
        return codecRegistry.get(BsonValueCodecProvider.getClassForBsonType(reader.getCurrentBsonType())).decode(reader, decoderContext);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void encode(final BsonWriter writer, final BsonValue value, final EncoderContext encoderContext) {
        Codec codec = codecRegistry.get(value.getClass());
        encoderContext.encodeWithChildContext(codec, writer, value);
    }

    @Override
    public Class<BsonValue> getEncoderClass() {
        return BsonValue.class;
    }
}
