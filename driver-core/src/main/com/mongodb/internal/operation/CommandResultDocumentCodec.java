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

package com.mongodb.internal.operation;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collections;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

class CommandResultDocumentCodec<T> extends BsonDocumentCodec {
    private final Decoder<T> payloadDecoder;
    private final List<String> fieldsContainingPayload;

    CommandResultDocumentCodec(final CodecRegistry registry, final Decoder<T> payloadDecoder, final List<String> fieldsContainingPayload) {
        super(registry);
        this.payloadDecoder = payloadDecoder;
        this.fieldsContainingPayload = fieldsContainingPayload;
    }

    static <P> Codec<BsonDocument> create(final Decoder<P> decoder, final String fieldContainingPayload) {
        return create(decoder, Collections.singletonList(fieldContainingPayload));
    }

    static <P> Codec<BsonDocument> create(final Decoder<P> decoder, final List<String> fieldsContainingPayload) {
        CodecRegistry registry = fromProviders(new CommandResultCodecProvider<P>(decoder, fieldsContainingPayload));
        return registry.get(BsonDocument.class);
    }

    @Override
    protected BsonValue readValue(final BsonReader reader, final DecoderContext decoderContext) {
        if (fieldsContainingPayload.contains(reader.getCurrentName())) {
            if (reader.getCurrentBsonType() == BsonType.DOCUMENT) {
                return new BsonDocumentWrapper<T>(payloadDecoder.decode(reader, decoderContext), null);
            } else if (reader.getCurrentBsonType() == BsonType.ARRAY) {
                return new CommandResultArrayCodec<T>(getCodecRegistry(), payloadDecoder).decode(reader, decoderContext);
            }
        }
        return super.readValue(reader, decoderContext);
    }
}

