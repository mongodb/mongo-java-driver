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
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.plus
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDbPointer
import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonJavaScriptWithScope
import org.bson.BsonMaxKey
import org.bson.BsonMinKey
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.BsonUndefined
import org.bson.BsonValue
import org.bson.RawBsonArray
import org.bson.RawBsonDocument
import org.bson.types.ObjectId

/**
 * The default serializers module
 *
 * Handles:
 * - ObjectId serialization
 * - BsonValue serialization
 */
@ExperimentalSerializationApi
public val defaultSerializersModule: SerializersModule =
    ObjectIdSerializer.serializersModule + BsonValueSerializer.serializersModule

@ExperimentalSerializationApi
@Serializer(forClass = ObjectId::class)
public object ObjectIdSerializer : KSerializer<ObjectId> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("ObjectIdSerializer", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: ObjectId) {
        when (encoder) {
            is BsonEncoder -> encoder.encodeObjectId(value)
            else -> throw SerializationException("ObjectId is not supported by ${encoder::class}")
        }
    }

    override fun deserialize(decoder: Decoder): ObjectId {
        return when (decoder) {
            is BsonDecoder -> decoder.decodeObjectId()
            else -> throw SerializationException("ObjectId is not supported by ${decoder::class}")
        }
    }

    public val serializersModule: SerializersModule = SerializersModule {
        contextual(ObjectId::class, ObjectIdSerializer)
    }
}

@ExperimentalSerializationApi
@Serializer(forClass = BsonValue::class)
public object BsonValueSerializer : KSerializer<BsonValue> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("BsonValueSerializer", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: BsonValue) {
        when (encoder) {
            is BsonEncoder -> encoder.encodeBsonValue(value)
            else -> throw SerializationException("BsonValues are not supported by ${encoder::class}")
        }
    }

    override fun deserialize(decoder: Decoder): BsonValue {
        return when (decoder) {
            is BsonDecoder -> decoder.decodeBsonValue()
            else -> throw SerializationException("BsonValues are not supported by ${decoder::class}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    public val serializersModule: SerializersModule = SerializersModule {
        contextual(BsonNull::class, BsonValueSerializer as KSerializer<BsonNull>)
        contextual(BsonArray::class, BsonValueSerializer as KSerializer<BsonArray>)
        contextual(BsonBinary::class, BsonValueSerializer as KSerializer<BsonBinary>)
        contextual(BsonBoolean::class, BsonValueSerializer as KSerializer<BsonBoolean>)
        contextual(BsonDateTime::class, BsonValueSerializer as KSerializer<BsonDateTime>)
        contextual(BsonDbPointer::class, BsonValueSerializer as KSerializer<BsonDbPointer>)
        contextual(BsonDocument::class, BsonValueSerializer as KSerializer<BsonDocument>)
        contextual(BsonDouble::class, BsonValueSerializer as KSerializer<BsonDouble>)
        contextual(BsonInt32::class, BsonValueSerializer as KSerializer<BsonInt32>)
        contextual(BsonInt64::class, BsonValueSerializer as KSerializer<BsonInt64>)
        contextual(BsonDecimal128::class, BsonValueSerializer as KSerializer<BsonDecimal128>)
        contextual(BsonMaxKey::class, BsonValueSerializer as KSerializer<BsonMaxKey>)
        contextual(BsonMinKey::class, BsonValueSerializer as KSerializer<BsonMinKey>)
        contextual(BsonJavaScript::class, BsonValueSerializer as KSerializer<BsonJavaScript>)
        contextual(BsonJavaScriptWithScope::class, BsonValueSerializer as KSerializer<BsonJavaScriptWithScope>)
        contextual(BsonObjectId::class, BsonValueSerializer as KSerializer<BsonObjectId>)
        contextual(BsonRegularExpression::class, BsonValueSerializer as KSerializer<BsonRegularExpression>)
        contextual(BsonString::class, BsonValueSerializer as KSerializer<BsonString>)
        contextual(BsonSymbol::class, BsonValueSerializer as KSerializer<BsonSymbol>)
        contextual(BsonTimestamp::class, BsonValueSerializer as KSerializer<BsonTimestamp>)
        contextual(BsonUndefined::class, BsonValueSerializer as KSerializer<BsonUndefined>)
        contextual(BsonDocument::class, BsonValueSerializer as KSerializer<BsonDocument>)
        contextual(RawBsonDocument::class, BsonValueSerializer as KSerializer<RawBsonDocument>)
        contextual(RawBsonArray::class, BsonValueSerializer as KSerializer<RawBsonArray>)
    }
}
