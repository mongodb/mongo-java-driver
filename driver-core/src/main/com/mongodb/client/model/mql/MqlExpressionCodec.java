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

package com.mongodb.client.model.mql;

import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

@SuppressWarnings("rawtypes")
final class MqlExpressionCodec implements Codec<MqlExpression> {
    private final CodecRegistry codecRegistry;

    MqlExpressionCodec(final CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    @Override
    public MqlExpression decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("Decoding to an MqlExpression is not supported");
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void encode(final BsonWriter writer, final MqlExpression value, final EncoderContext encoderContext) {
        BsonValue bsonValue = value.toBsonValue(codecRegistry);
        Codec codec = codecRegistry.get(bsonValue.getClass());
        codec.encode(writer, bsonValue, encoderContext);
    }

    @Override
    public Class<MqlExpression> getEncoderClass() {
        return MqlExpression.class;
    }
}
