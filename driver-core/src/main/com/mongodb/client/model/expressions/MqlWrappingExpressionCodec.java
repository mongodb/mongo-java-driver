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

package com.mongodb.client.model.expressions;

import com.mongodb.client.model.expressions.MqlExpression.MqlWrappingExpression;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

@SuppressWarnings("rawtypes")
final class MqlWrappingExpressionCodec implements Codec<MqlWrappingExpression> {
    private final CodecRegistry codecRegistry;

    MqlWrappingExpressionCodec(final CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    @Override
    public MqlWrappingExpression decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("Decoding to an expression is not supported");
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void encode(final BsonWriter writer, final MqlWrappingExpression value, final EncoderContext encoderContext) {
        BsonValue bsonValue = value.getWrapped().toBsonValue((codecRegistry));
        Codec codec = codecRegistry.get(bsonValue.getClass());
        codec.encode(writer, bsonValue, encoderContext);
    }

    @Override
    public Class<MqlWrappingExpression> getEncoderClass() {
        return MqlWrappingExpression.class;
    }
}
