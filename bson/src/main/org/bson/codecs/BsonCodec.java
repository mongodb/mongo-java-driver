/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs;

import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static java.lang.String.format;

/**
 * A codec for encoding Bson Implementations
 *
 * @since 3.11
 */
public class BsonCodec implements Codec<Bson> {
    private static final Codec<BsonDocument> BSON_DOCUMENT_CODEC = new BsonDocumentCodec();
    private final CodecRegistry registry;

    /**
     * Create a new instance
     *
     * @param registry the codec registry
     */
    public BsonCodec(final CodecRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Bson decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("The BsonCodec can only encode to Bson");
    }

    @Override
    public void encode(final BsonWriter writer, final Bson value, final EncoderContext encoderContext) {
        try {
            BsonDocument bsonDocument = value.toBsonDocument(BsonDocument.class, registry);
            BSON_DOCUMENT_CODEC.encode(writer, bsonDocument, encoderContext);
        } catch (Exception e) {
            throw new CodecConfigurationException(format("Unable to encode a Bson implementation: %s", value), e);
        }
    }

    @Override
    public Class<Bson> getEncoderClass() {
        return Bson.class;
    }
}
