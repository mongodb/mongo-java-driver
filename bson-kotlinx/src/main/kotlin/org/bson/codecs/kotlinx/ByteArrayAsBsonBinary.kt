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
package org.bson.codecs.kotlinx

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import org.bson.BsonBinary

/**
 * ByteArray KSerializer.
 *
 * Encodes and decodes `ByteArray` values to and from a compact `BsonBinary` (subtype
 * [org.bson.BsonBinarySubType.BINARY]), matching the behavior of the standard `ByteArrayCodec` and the `bson-kotlin`
 * `DataClassCodec`.
 *
 * This is an opt-in serializer: kotlinx.serialization's built-in `ByteArray` serializer encodes a `ByteArray` as a BSON
 * array of int32 elements (one element per byte). To store a `ByteArray` field as `BsonBinary` instead, either annotate
 * the field with `@Serializable(with = ByteArrayAsBsonBinary::class)`, or annotate it with `@Contextual` and register
 * [ByteArrayAsBsonBinary.serializersModule] on the codec.
 *
 * @since 5.10
 */
@ExperimentalSerializationApi
public object ByteArrayAsBsonBinary : KSerializer<ByteArray> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("ByteArrayAsBsonBinary", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: ByteArray) {
        when (encoder) {
            is BsonEncoder -> encoder.encodeBsonValue(BsonBinary(value))
            else -> throw SerializationException("ByteArray is not supported by ${encoder::class}")
        }
    }

    override fun deserialize(decoder: Decoder): ByteArray {
        return when (decoder) {
            is BsonDecoder -> decoder.decodeBsonValue().asBinary().data
            else -> throw SerializationException("ByteArray is not supported by ${decoder::class}")
        }
    }

    public val serializersModule: SerializersModule = SerializersModule {
        contextual(ByteArray::class, ByteArrayAsBsonBinary)
    }
}
