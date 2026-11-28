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

import java.util.Base64
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.modules.SerializersModule
import org.bson.AbstractBsonReader
import org.bson.BsonBinarySubType
import org.bson.BsonType
import org.bson.UuidRepresentation
import org.bson.codecs.kotlinx.utils.BsonCodecUtils.asJsonNamingStrategy
import org.bson.internal.UuidHelper

@OptIn(ExperimentalSerializationApi::class)
internal interface JsonBsonDecoder : BsonDecoder, JsonDecoder {
    val reader: AbstractBsonReader
    val configuration: BsonConfiguration

    fun json(): Json = Json {
        explicitNulls = configuration.explicitNulls
        encodeDefaults = configuration.encodeDefaults
        classDiscriminator = configuration.classDiscriminator
        namingStrategy = configuration.bsonNamingStrategy.asJsonNamingStrategy()
        serializersModule = this@JsonBsonDecoder.serializersModule
    }

    @Suppress("ComplexMethod")
    override fun decodeJsonElement(): JsonElement =
        reader.run {
            when (currentBsonType) {
                BsonType.DOCUMENT -> readJsonObject()
                BsonType.ARRAY -> readJsonArray()
                BsonType.NULL -> JsonPrimitive(decodeNull())
                BsonType.STRING -> JsonPrimitive(decodeString())
                BsonType.BOOLEAN -> JsonPrimitive(decodeBoolean())
                BsonType.INT32 -> JsonPrimitive(decodeInt())
                BsonType.INT64 -> JsonPrimitive(decodeLong())
                BsonType.DOUBLE -> JsonPrimitive(decodeDouble())
                BsonType.DECIMAL128 -> JsonPrimitive(reader.readDecimal128())
                BsonType.OBJECT_ID -> JsonPrimitive(decodeObjectId().toHexString())
                BsonType.DATE_TIME -> JsonPrimitive(reader.readDateTime())
                BsonType.TIMESTAMP -> JsonPrimitive(reader.readTimestamp().value)
                BsonType.BINARY -> {
                    val subtype = reader.peekBinarySubType()
                    val data = reader.readBinaryData().data
                    when (subtype) {
                        BsonBinarySubType.UUID_LEGACY.value ->
                            JsonPrimitive(
                                UuidHelper.decodeBinaryToUuid(data, subtype, UuidRepresentation.JAVA_LEGACY).toString())
                        BsonBinarySubType.UUID_STANDARD.value ->
                            JsonPrimitive(
                                UuidHelper.decodeBinaryToUuid(data, subtype, UuidRepresentation.STANDARD).toString())
                        else -> JsonPrimitive(Base64.getEncoder().encodeToString(data))
                    }
                }
                else -> error("Unsupported json type: $currentBsonType")
            }
        }

    private fun readJsonObject(): JsonObject {
        reader.readStartDocument()
        val obj = buildJsonObject {
            var type = reader.readBsonType()
            while (type != BsonType.END_OF_DOCUMENT) {
                put(reader.readName(), decodeJsonElement())
                type = reader.readBsonType()
            }
        }

        reader.readEndDocument()
        return obj
    }

    private fun readJsonArray(): JsonArray {
        reader.readStartArray()
        val array = buildJsonArray {
            var type = reader.readBsonType()
            while (type != BsonType.END_OF_DOCUMENT) {
                add(decodeJsonElement())
                type = reader.readBsonType()
            }
        }

        reader.readEndArray()
        return array
    }
}

internal class JsonBsonDecoderImpl(
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : BsonDecoderImpl(reader, serializersModule, configuration), JsonBsonDecoder {
    override val json = json()
}

internal class JsonBsonArrayDecoder(
    descriptor: SerialDescriptor,
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : BsonArrayDecoder(descriptor, reader, serializersModule, configuration), JsonBsonDecoder {
    override val json = json()
}

internal class JsonBsonDocumentDecoder(
    descriptor: SerialDescriptor,
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : BsonDocumentDecoder(descriptor, reader, serializersModule, configuration), JsonBsonDecoder {
    override val json = json()
}

internal class JsonBsonPolymorphicDecoder(
    descriptor: SerialDescriptor,
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : BsonPolymorphicDecoder(descriptor, reader, serializersModule, configuration), JsonBsonDecoder {
    override val json = json()
}

internal class JsonBsonMapDecoder(
    descriptor: SerialDescriptor,
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : BsonMapDecoder(descriptor, reader, serializersModule, configuration), JsonBsonDecoder {
    override val json = json()
}
