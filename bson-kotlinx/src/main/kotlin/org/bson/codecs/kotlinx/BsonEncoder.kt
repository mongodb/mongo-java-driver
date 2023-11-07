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
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
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
import org.bson.BsonValue
import org.bson.BsonWriter
import org.bson.codecs.BsonValueCodec
import org.bson.codecs.EncoderContext
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import java.math.BigDecimal

/**
 * The BsonEncoder interface
 *
 * For custom serialization handlers
 */
public sealed interface BsonEncoder {

    /**
     * Encodes an ObjectId
     *
     * @param value the ObjectId
     */
    public fun encodeObjectId(value: ObjectId)

    /**
     * Encodes a BsonValue
     *
     * @param value the BsonValue
     */
    public fun encodeBsonValue(value: BsonValue)

    /** @return the BsonWriter */
    public fun writer(): BsonWriter
}

@ExperimentalSerializationApi
internal class DefaultBsonEncoder(
    private val writer: BsonWriter,
    override val serializersModule: SerializersModule,
    private val configuration: BsonConfiguration
) : BsonEncoder, JsonEncoder, AbstractEncoder() {

    companion object {
        val validKeyKinds = setOf(PrimitiveKind.STRING, PrimitiveKind.CHAR, SerialKind.ENUM)
        val bsonValueCodec = BsonValueCodec()
        private val DOUBLE_MIN_VALUE = BigDecimal.valueOf(Double.MIN_VALUE)
        private val DOUBLE_MAX_VALUE = BigDecimal.valueOf(Double.MAX_VALUE)
        private val INT_MIN_VALUE = BigDecimal.valueOf(Int.MIN_VALUE.toLong())
        private val INT_MAX_VALUE = BigDecimal.valueOf(Int.MAX_VALUE.toLong())
        private val LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE)
        private val LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE)
    }

    private var isPolymorphic = false
    private var state = STATE.VALUE
    private var mapState = MapState()
    private var deferredElementName: String? = null
    override val json = Json {
        explicitNulls = configuration.explicitNulls
        encodeDefaults = configuration.encodeDefaults
        classDiscriminator = configuration.classDiscriminator
        serializersModule = this@DefaultBsonEncoder.serializersModule
    }

    override fun shouldEncodeElementDefault(descriptor: SerialDescriptor, index: Int): Boolean =
        configuration.encodeDefaults

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        when (descriptor.kind) {
            is StructureKind.LIST -> writer.writeStartArray()
            is PolymorphicKind -> {
                writer.writeStartDocument()
                writer.writeName(configuration.classDiscriminator)
                isPolymorphic = true
            }
            is StructureKind.CLASS,
            StructureKind.OBJECT -> {
                if (isPolymorphic) {
                    isPolymorphic = false
                } else {
                    writer.writeStartDocument()
                }
            }
            is StructureKind.MAP -> {
                writer.writeStartDocument()
                mapState = MapState()
            }
            else -> throw SerializationException("Primitives are not supported at top-level")
        }
        return super.beginStructure(descriptor)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        when (descriptor.kind) {
            is StructureKind.LIST -> writer.writeEndArray()
            StructureKind.MAP,
            StructureKind.CLASS,
            StructureKind.OBJECT -> writer.writeEndDocument()
            else -> super.endStructure(descriptor)
        }
    }

    override fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
        when (descriptor.kind) {
            is StructureKind.CLASS -> {
                val elementName = descriptor.getElementName(index)
                if (descriptor.getElementDescriptor(index).isNullable) {
                    deferredElementName = elementName
                } else {
                    encodeName(elementName)
                }
            }
            is StructureKind.MAP -> {
                if (index == 0) {
                    val keyKind = descriptor.getElementDescriptor(index).kind
                    if (!validKeyKinds.contains(keyKind)) {
                        throw SerializationException(
                            """Invalid key type for ${descriptor.serialName}.
                                | Expected STRING or ENUM but found: `${keyKind}`."""
                                .trimMargin())
                    }
                }
                state = mapState.nextState()
            }
            else -> {}
        }
        return true
    }

    override fun <T : Any> encodeNullableSerializableValue(serializer: SerializationStrategy<T>, value: T?) {
        deferredElementName?.let {
            if (value != null || configuration.explicitNulls) {
                encodeName(it)
                super<AbstractEncoder>.encodeNullableSerializableValue(serializer, value)
            }
        }
            ?: super<AbstractEncoder>.encodeNullableSerializableValue(serializer, value)
    }

    override fun encodeByte(value: Byte) = encodeInt(value.toInt())
    override fun encodeChar(value: Char) = encodeString(value.toString())
    override fun encodeFloat(value: Float) = encodeDouble(value.toDouble())
    override fun encodeShort(value: Short) = encodeInt(value.toInt())

    override fun encodeBoolean(value: Boolean) = writer.writeBoolean(value)
    override fun encodeDouble(value: Double) = writer.writeDouble(value)
    override fun encodeInt(value: Int) = writer.writeInt32(value)
    override fun encodeLong(value: Long) = writer.writeInt64(value)
    override fun encodeNull() = writer.writeNull()

    override fun encodeString(value: String) {
        when (state) {
            STATE.NAME -> encodeName(value)
            STATE.VALUE -> writer.writeString(value)
        }
    }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        val value = enumDescriptor.getElementName(index)
        when (state) {
            STATE.NAME -> encodeName(value)
            STATE.VALUE -> writer.writeString(value)
        }
    }

    override fun encodeObjectId(value: ObjectId) {
        writer.writeObjectId(value)
    }

    override fun encodeBsonValue(value: BsonValue) {
        bsonValueCodec.encode(writer, value, EncoderContext.builder().build())
    }

    override fun encodeJsonElement(element: JsonElement) = when(element) {
        is JsonNull -> encodeNull()
        is JsonPrimitive -> {
            val content = element.content
            when {
                element.isString -> encodeString(content)
                content == "true" || content == "false" ->
                    encodeBoolean(content.toBooleanStrict())
                else -> {
                    val decimal = BigDecimal(content)
                    when  {
                        decimal.stripTrailingZeros().scale() > 0 ->
                            if (DOUBLE_MIN_VALUE <= decimal && decimal <= DOUBLE_MAX_VALUE) {
                                encodeDouble(element.double)
                            } else {
                                writer.writeDecimal128(Decimal128(decimal))
                            }
                        INT_MIN_VALUE <= decimal && decimal <= INT_MAX_VALUE ->
                            encodeInt(element.int)
                        LONG_MIN_VALUE <= decimal && decimal <= LONG_MAX_VALUE ->
                            encodeLong(element.long)
                        else -> writer.writeDecimal128(Decimal128(decimal))
                    }
                }
            }
        }
        is JsonObject -> encodeJsonObject(element)
        is JsonArray -> encodeJsonArray(element)
    }

    override fun writer(): BsonWriter = writer

    private fun encodeJsonObject(obj: JsonObject) {
        writer.writeStartDocument()
        obj.forEach { k, v ->
            writer.writeName(k)
            encodeJsonElement(v)
        }
        writer.writeEndDocument()
    }

    private fun encodeJsonArray(array: JsonArray) {
        writer.writeStartArray()
        array.forEach(::encodeJsonElement)
        writer.writeEndArray()
    }

    private fun encodeName(value: Any) {
        writer.writeName(value.toString())
        deferredElementName = null
        state = STATE.VALUE
    }

    private enum class STATE {
        NAME,
        VALUE
    }

    private class MapState {
        var currentState: STATE = STATE.VALUE

        fun getState(): STATE = currentState

        fun nextState(): STATE {
            currentState =
                when (currentState) {
                    STATE.VALUE -> STATE.NAME
                    STATE.NAME -> STATE.VALUE
                }
            return getState()
        }
    }
}
