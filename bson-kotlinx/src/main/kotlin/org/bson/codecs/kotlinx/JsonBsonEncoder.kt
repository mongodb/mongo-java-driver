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

import java.math.BigDecimal
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.long
import kotlinx.serialization.modules.SerializersModule
import org.bson.BsonWriter
import org.bson.codecs.kotlinx.utils.BsonCodecUtils.toJsonNamingStrategy
import org.bson.types.Decimal128

@OptIn(ExperimentalSerializationApi::class)
internal class JsonBsonEncoder(
    writer: BsonWriter,
    override val serializersModule: SerializersModule,
    configuration: BsonConfiguration,
) : BsonEncoderImpl(writer, serializersModule, configuration), JsonEncoder {

    companion object {
        private val DOUBLE_MIN_VALUE = BigDecimal.valueOf(Double.MIN_VALUE)
        private val DOUBLE_MAX_VALUE = BigDecimal.valueOf(Double.MAX_VALUE)
        private val INT_MIN_VALUE = BigDecimal.valueOf(Int.MIN_VALUE.toLong())
        private val INT_MAX_VALUE = BigDecimal.valueOf(Int.MAX_VALUE.toLong())
        private val LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE)
        private val LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE)
    }

    override val json = Json {
        explicitNulls = configuration.explicitNulls
        encodeDefaults = configuration.encodeDefaults
        classDiscriminator = configuration.classDiscriminator
        namingStrategy = configuration.bsonNamingStrategy.toJsonNamingStrategy()
        serializersModule = this@JsonBsonEncoder.serializersModule
    }

    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        if (value is JsonElement) encodeJsonElement(value)
        else super<BsonEncoderImpl>.encodeSerializableValue(serializer, value)
    }

    override fun encodeJsonElement(element: JsonElement) {
        deferredElementHandler.with(
            {
                when (element) {
                    is JsonNull ->
                        if (configuration.explicitNulls) {
                            encodeName(it)
                            encodeNull()
                        }
                    is JsonPrimitive -> {
                        encodeName(it)
                        encodeJsonPrimitive(element)
                    }
                    is JsonObject -> {
                        encodeName(it)
                        encodeJsonObject(element)
                    }
                    is JsonArray -> {
                        encodeName(it)
                        encodeJsonArray(element)
                    }
                }
            },
            {
                when (element) {
                    is JsonNull -> if (configuration.explicitNulls) encodeNull()
                    is JsonPrimitive -> encodeJsonPrimitive(element)
                    is JsonObject -> encodeJsonObject(element)
                    is JsonArray -> encodeJsonArray(element)
                }
            })
    }

    private fun encodeJsonPrimitive(primitive: JsonPrimitive) {
        val content = primitive.content
        when {
            primitive.isString -> encodeString(content)
            content == "true" || content == "false" -> encodeBoolean(content.toBooleanStrict())
            else -> {
                val decimal = BigDecimal(content)
                when {
                    decimal.scale() != 0 ->
                        if (DOUBLE_MIN_VALUE <= decimal && decimal <= DOUBLE_MAX_VALUE) {
                            encodeDouble(primitive.double)
                        } else {
                            writer.writeDecimal128(Decimal128(decimal))
                        }
                    INT_MIN_VALUE <= decimal && decimal <= INT_MAX_VALUE -> encodeInt(primitive.int)
                    LONG_MIN_VALUE <= decimal && decimal <= LONG_MAX_VALUE -> encodeLong(primitive.long)
                    else -> writer.writeDecimal128(Decimal128(decimal))
                }
            }
        }
    }

    private fun encodeJsonObject(obj: JsonObject) {
        writer.writeStartDocument()
        obj.forEach { k, v ->
            deferredElementHandler.set(k)
            encodeJsonElement(v)
        }
        writer.writeEndDocument()
    }

    private fun encodeJsonArray(array: JsonArray) {
        writer.writeStartArray()
        array.forEach(::encodeJsonElement)
        writer.writeEndArray()
    }
}
