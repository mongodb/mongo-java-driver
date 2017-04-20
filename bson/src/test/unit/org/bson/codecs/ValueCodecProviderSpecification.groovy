/*
 * Copyright 2015 MongoDB, Inc.
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

package org.bson.codecs

import org.bson.Document
import org.bson.codecs.configuration.CodecRegistries
import org.bson.types.Binary
import org.bson.types.Code
import org.bson.types.Decimal128
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import org.bson.types.Symbol
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

class ValueCodecProviderSpecification extends Specification {
    private final provider = new ValueCodecProvider()
    private final registry = CodecRegistries.fromProviders(provider)

    def 'should provide supported codecs'() {
        expect:
        provider.get(AtomicBoolean, registry) instanceof AtomicBooleanCodec
        provider.get(AtomicInteger, registry) instanceof AtomicIntegerCodec
        provider.get(AtomicLong, registry) instanceof AtomicLongCodec

        provider.get(Boolean, registry) instanceof BooleanCodec
        provider.get(Integer, registry) instanceof IntegerCodec
        provider.get(Long, registry) instanceof LongCodec
        provider.get(Decimal128, registry) instanceof Decimal128Codec
        provider.get(BigDecimal, registry) instanceof BigDecimalCodec
        provider.get(Double, registry) instanceof DoubleCodec
        provider.get(Character, registry) instanceof CharacterCodec
        provider.get(String, registry) instanceof StringCodec
        provider.get(Date, registry) instanceof DateCodec
        provider.get(Byte, registry) instanceof ByteCodec
        provider.get(Pattern, registry) instanceof PatternCodec
        provider.get(Short, registry) instanceof ShortCodec
        provider.get(byte[], registry) instanceof ByteArrayCodec
        provider.get(Float, registry) instanceof FloatCodec

        provider.get(Binary, registry) instanceof BinaryCodec
        provider.get(MinKey, registry) instanceof MinKeyCodec
        provider.get(MaxKey, registry) instanceof MaxKeyCodec
        provider.get(Code, registry) instanceof CodeCodec
        provider.get(ObjectId, registry) instanceof ObjectIdCodec
        provider.get(Symbol, registry) instanceof SymbolCodec
        provider.get(UUID, registry) instanceof UuidCodec

        provider.get(Document, registry) == null
    }

}
