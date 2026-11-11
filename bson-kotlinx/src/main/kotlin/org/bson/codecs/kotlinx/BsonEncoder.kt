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
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import org.bson.BsonValue
import org.bson.BsonWriter
import org.bson.codecs.BsonValueCodec
import org.bson.codecs.EncoderContext
import org.bson.types.ObjectId

/**
 * The BsonEncoder interface
 *
 * For custom serialization handlers
 */
@ExperimentalSerializationApi
public sealed interface BsonEncoder : Encoder, CompositeEncoder {

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
}

/**
 * The default BsonEncoder implementation
 *
 * Unlike BsonDecoder implementations, state is shared when encoding, so a single class is used to encode Bson Arrays,
 * Documents, Polymorphic types and Maps.
 */
@OptIn(ExperimentalSerializationApi::class)
internal open class BsonEncoderImpl(
    val writer: BsonWriter,
    override val serializersModule: SerializersModule,
    val configuration: BsonConfiguration
) : BsonEncoder, AbstractEncoder() {

    companion object {
        val validKeyKinds = setOf(PrimitiveKind.STRING, PrimitiveKind.CHAR, SerialKind.ENUM)
        val bsonValueCodec = BsonValueCodec()
    }

    private var isPolymorphic = false
    private var state = STATE.VALUE
    private var mapState = MapState()
    internal val deferredElementHandler: DeferredElementHandler = DeferredElementHandler()

    override fun shouldEncodeElementDefault(descriptor: SerialDescriptor, index: Int): Boolean =
        configuration.encodeDefaults

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        when (descriptor.kind) {
            is PolymorphicKind -> {
                writer.writeStartDocument()
                writer.writeName(configuration.classDiscriminator)
                isPolymorphic = true
            }
            is StructureKind.LIST -> writer.writeStartArray()
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
        return this
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        when (descriptor.kind) {
            is StructureKind.LIST -> writer.writeEndArray()
            StructureKind.MAP,
            StructureKind.CLASS,
            StructureKind.OBJECT -> writer.writeEndDocument()
            else -> {}
        }
    }

    override fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
        when (descriptor.kind) {
            is StructureKind.CLASS -> {
                val elementName = descriptor.getElementName(index)
                if (descriptor.getElementDescriptor(index).isNullable) {
                    deferredElementHandler.set(elementName)
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

    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        deferredElementHandler.with(
            {
                // When using generics its possible for `value` to be null
                // See: https://youtrack.jetbrains.com/issue/KT-66206
                if (value != null || configuration.explicitNulls) {
                    encodeName(it)
                    super<AbstractEncoder>.encodeSerializableValue(serializer, value)
                }
            },
            { super<AbstractEncoder>.encodeSerializableValue(serializer, value) })
    }

    override fun <T : Any> encodeNullableSerializableValue(serializer: SerializationStrategy<T>, value: T?) {
        deferredElementHandler.with(
            {
                if (value != null || configuration.explicitNulls) {
                    encodeName(it)
                    super<AbstractEncoder>.encodeNullableSerializableValue(serializer, value)
                }
            },
            { super<AbstractEncoder>.encodeNullableSerializableValue(serializer, value) })
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
            STATE.NAME -> deferredElementHandler.set(value)
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

    internal fun encodeName(value: Any) {
        val name = value.toString().let { configuration.bsonNamingStrategy?.transformName(it) ?: it }
        writer.writeName(name)
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

    internal class DeferredElementHandler {
        private var deferredElementName: String? = null

        fun set(name: String) {
            assert(deferredElementName == null) { "Overwriting an existing deferred name" }
            deferredElementName = name
        }

        fun with(actionWithDeferredElement: (String) -> Unit, actionWithoutDeferredElement: () -> Unit) {
            deferredElementName?.let {
                reset()
                actionWithDeferredElement(it)
            }
                ?: actionWithoutDeferredElement()
        }

        private fun reset() {
            deferredElementName = null
        }
    }
}
