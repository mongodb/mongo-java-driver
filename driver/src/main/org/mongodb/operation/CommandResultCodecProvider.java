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

package org.mongodb.operation;

import org.bson.codecs.BinaryCodec;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.BsonBooleanCodec;
import org.bson.codecs.BsonDateTimeCodec;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonDoubleCodec;
import org.bson.codecs.BsonInt32Codec;
import org.bson.codecs.BsonInt64Codec;
import org.bson.codecs.BsonNullCodec;
import org.bson.codecs.BsonStringCodec;
import org.bson.codecs.CodeCodec;
import org.bson.codecs.CodeWithScopeCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DBPointerCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.MaxKeyCodec;
import org.bson.codecs.MinKeyCodec;
import org.bson.codecs.ObjectIdCodec;
import org.bson.codecs.RegularExpressionCodec;
import org.bson.codecs.SymbolCodec;
import org.bson.codecs.TimestampCodec;
import org.bson.codecs.UndefinedCodec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BsonArray;
import org.bson.types.BsonDocument;

import java.util.HashMap;
import java.util.Map;

class CommandResultCodecProvider<P> implements CodecProvider {
    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();
    private final Decoder<P> payloadDecoder;
    private final String fieldContainingPayload;

    /**
     * Construct a new instance with the default codec for each BSON type.
     */
    public CommandResultCodecProvider(final Decoder<P> payloadDecoder, final String fieldContainingPayload) {
        this.payloadDecoder = payloadDecoder;
        this.fieldContainingPayload = fieldContainingPayload;
        addCodecs();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (codecs.containsKey(clazz)) {
            return (Codec<T>) codecs.get(clazz);
        }

        if (clazz == BsonArray.class) {
            return (Codec<T>) new BsonArrayCodec(registry);
        }

        if (clazz == BsonDocument.class) {
            return (Codec<T>) new CommandResultDocumentCodec<P>(registry, payloadDecoder, fieldContainingPayload);
        }

        return null;
    }

    private void addCodecs() {
        addCodec(new BsonNullCodec());
        addCodec(new BinaryCodec());
        addCodec(new BsonBooleanCodec());
        addCodec(new BsonDateTimeCodec());
        addCodec(new DBPointerCodec());
        addCodec(new BsonDoubleCodec());
        addCodec(new BsonInt32Codec());
        addCodec(new BsonInt64Codec());
        addCodec(new MinKeyCodec());
        addCodec(new MaxKeyCodec());
        addCodec(new CodeCodec());
        addCodec(new ObjectIdCodec());
        addCodec(new RegularExpressionCodec());
        addCodec(new BsonStringCodec());
        addCodec(new SymbolCodec());
        addCodec(new TimestampCodec());
        addCodec(new UndefinedCodec());
        addCodec(new CodeWithScopeCodec(new BsonDocumentCodec()));
    }

    private <T> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }
}
