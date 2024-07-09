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

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.CompositeDecoder.Companion.DECODE_DONE
import kotlinx.serialization.encoding.CompositeDecoder.Companion.UNKNOWN_NAME
import kotlinx.serialization.modules.SerializersModule
import org.bson.AbstractBsonReader
import org.bson.BsonInvalidOperationException
import org.bson.BsonReader
import org.bson.BsonReaderMark
import org.bson.BsonType
import org.bson.BsonValue
import org.bson.codecs.BsonValueCodec
import org.bson.codecs.DecoderContext
import org.bson.internal.NumberCodecHelper
import org.bson.internal.StringCodecHelper
import org.bson.types.ObjectId

/**
 * The BsonDecoder interface
 *
 * For custom serialization handlers
 */
public sealed interface BsonDecoder {

    /** @return the decoded ObjectId */
    public fun decodeObjectId(): ObjectId
    /** @return the decoded BsonValue */
    public fun decodeBsonValue(): BsonValue

    /** @return the BsonReader */
    public fun reader(): BsonReader
}

@ExperimentalSerializationApi
internal open class DefaultBsonDecoder(
    internal val reader: AbstractBsonReader,
    override val serializersModule: SerializersModule,
    internal val configuration: BsonConfiguration
) : BsonDecoder, AbstractDecoder() {

    private data class ElementMetadata(val name: String, val nullable: Boolean, var processed: Boolean = false)
    private var elementsMetadata: Array<ElementMetadata>? = null
    private var currentIndex: Int = UNKNOWN_INDEX

    companion object {
        val validKeyKinds = setOf(PrimitiveKind.STRING, PrimitiveKind.CHAR, SerialKind.ENUM)
        val bsonValueCodec = BsonValueCodec()
        const val UNKNOWN_INDEX = -10
        fun validateCurrentBsonType(
            reader: AbstractBsonReader,
            expectedType: BsonType,
            descriptor: SerialDescriptor,
            actualType: (descriptor: SerialDescriptor) -> String = { it.kind.toString() }
        ) {
            reader.currentBsonType?.let {
                if (it != expectedType) {
                    throw SerializationException(
                        "Invalid data for `${actualType(descriptor)}` expected a bson " +
                            "${expectedType.name.lowercase()} found: ${reader.currentBsonType}")
                }
            }
        }
    }

    private fun initElementMetadata(descriptor: SerialDescriptor) {
        if (this.elementsMetadata != null) return
        val elementsMetadata =
            Array(descriptor.elementsCount) {
                val elementDescriptor = descriptor.getElementDescriptor(it)
                ElementMetadata(
                    elementDescriptor.serialName, elementDescriptor.isNullable && !descriptor.isElementOptional(it))
            }
        this.elementsMetadata = elementsMetadata
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        initElementMetadata(descriptor)
        currentIndex = decodeElementIndexImpl(descriptor)
        elementsMetadata?.getOrNull(currentIndex)?.processed = true
        return currentIndex
    }

    @Suppress("ReturnCount", "ComplexMethod")
    private fun decodeElementIndexImpl(descriptor: SerialDescriptor): Int {
        val elementMetadata = elementsMetadata ?: error("elementsMetadata may not be null.")
        val name: String? =
            when (reader.state ?: error("State of reader may not be null.")) {
                AbstractBsonReader.State.NAME -> reader.readName()
                AbstractBsonReader.State.VALUE -> reader.currentName
                AbstractBsonReader.State.TYPE -> {
                    reader.readBsonType()
                    return decodeElementIndexImpl(descriptor)
                }
                AbstractBsonReader.State.END_OF_DOCUMENT,
                AbstractBsonReader.State.END_OF_ARRAY ->
                    return elementMetadata.indexOfFirst { it.nullable && !it.processed }
                else -> null
            }

        return name?.let {
            val index = descriptor.getElementIndex(it)
            return if (index == UNKNOWN_NAME) {
                reader.skipValue()
                decodeElementIndexImpl(descriptor)
            } else {
                index
            }
        }
            ?: UNKNOWN_NAME
    }

    @Suppress("ReturnCount")
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            is StructureKind.LIST -> BsonArrayDecoder(descriptor, reader, serializersModule, configuration)
            is PolymorphicKind -> PolymorphicDecoder(descriptor, reader, serializersModule, configuration)
            is StructureKind.CLASS,
            StructureKind.OBJECT -> BsonDocumentDecoder(descriptor, reader, serializersModule, configuration)
            is StructureKind.MAP -> MapDecoder(descriptor, reader, serializersModule, configuration)
            else -> throw SerializationException("Primitives are not supported at top-level")
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        when (descriptor.kind) {
            is StructureKind.LIST -> reader.readEndArray()
            is StructureKind.MAP,
            StructureKind.CLASS,
            StructureKind.OBJECT -> reader.readEndDocument()
            else -> super.endStructure(descriptor)
        }
    }

    override fun decodeByte(): Byte = NumberCodecHelper.decodeByte(reader)

    override fun decodeChar(): Char = StringCodecHelper.decodeChar(reader)
    override fun decodeFloat(): Float = NumberCodecHelper.decodeFloat(reader)

    override fun decodeShort(): Short = NumberCodecHelper.decodeShort(reader)
    override fun decodeBoolean(): Boolean = reader.readBoolean()

    override fun decodeDouble(): Double = NumberCodecHelper.decodeDouble(reader)
    override fun decodeInt(): Int = NumberCodecHelper.decodeInt(reader)
    override fun decodeLong(): Long = NumberCodecHelper.decodeLong(reader)
    override fun decodeString(): String = reader.readString()

    override fun decodeNull(): Nothing? {
        if (reader.state == AbstractBsonReader.State.VALUE) {
            readOrThrow({ reader.readNull() }, BsonType.NULL)
        }
        return null
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int = enumDescriptor.getElementIndex(decodeString())
    override fun decodeNotNullMark(): Boolean {
        return reader.state != AbstractBsonReader.State.END_OF_DOCUMENT && reader.currentBsonType != BsonType.NULL
    }

    override fun decodeObjectId(): ObjectId = reader.readObjectId()
    override fun decodeBsonValue(): BsonValue = bsonValueCodec.decode(reader, DecoderContext.builder().build())
    override fun reader(): BsonReader = reader

    private inline fun <T> readOrThrow(action: () -> T, bsonType: BsonType): T {
        return try {
            action()
        } catch (e: BsonInvalidOperationException) {
            throw BsonInvalidOperationException(
                "Reading field '${reader.currentName}' failed expected $bsonType type but found:" +
                    " ${reader.currentBsonType}.",
                e)
        }
    }
}

@OptIn(ExperimentalSerializationApi::class)
private class BsonArrayDecoder(
    descriptor: SerialDescriptor,
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : DefaultBsonDecoder(reader, serializersModule, configuration) {

    init {
        validateCurrentBsonType(reader, BsonType.ARRAY, descriptor)
        reader.readStartArray()
    }

    private var index = 0
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        val nextType = reader.readBsonType()
        if (nextType == BsonType.END_OF_DOCUMENT) return DECODE_DONE
        return index++
    }
}

@OptIn(ExperimentalSerializationApi::class)
private class PolymorphicDecoder(
    descriptor: SerialDescriptor,
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : DefaultBsonDecoder(reader, serializersModule, configuration) {
    private var index = 0
    private var mark: BsonReaderMark?

    init {
        mark = reader.mark
        validateCurrentBsonType(reader, BsonType.DOCUMENT, descriptor) { it.serialName }
        reader.readStartDocument()
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        mark?.let {
            it.reset()
            mark = null
        }
        return deserializer.deserialize(DefaultBsonDecoder(reader, serializersModule, configuration))
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        var found = false
        return when (index) {
            0 -> {
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    if (reader.readName() == configuration.classDiscriminator) {
                        found = true
                        break
                    }
                    reader.skipValue()
                }
                if (!found) {
                    throw SerializationException(
                        "Missing required discriminator field `${configuration.classDiscriminator}` " +
                            "for polymorphic class: `${descriptor.serialName}`.")
                }
                index++
            }
            1 -> index++
            else -> DECODE_DONE
        }
    }
}

@OptIn(ExperimentalSerializationApi::class)
private class BsonDocumentDecoder(
    descriptor: SerialDescriptor,
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : DefaultBsonDecoder(reader, serializersModule, configuration) {
    init {
        validateCurrentBsonType(reader, BsonType.DOCUMENT, descriptor) { it.serialName }
        reader.readStartDocument()
    }
}

@OptIn(ExperimentalSerializationApi::class)
private class MapDecoder(
    descriptor: SerialDescriptor,
    reader: AbstractBsonReader,
    serializersModule: SerializersModule,
    configuration: BsonConfiguration
) : DefaultBsonDecoder(reader, serializersModule, configuration) {

    private var index = 0
    private var isKey = false

    init {
        validateCurrentBsonType(reader, BsonType.DOCUMENT, descriptor)
        reader.readStartDocument()
    }

    override fun decodeString(): String {
        return if (isKey) {
            reader.readName()
        } else {
            super.decodeString()
        }
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        val keyKind = descriptor.getElementDescriptor(0).kind
        if (!validKeyKinds.contains(keyKind)) {
            throw SerializationException(
                "Invalid key type for ${descriptor.serialName}. Expected STRING or ENUM but found: `${keyKind}`")
        }

        if (!isKey) {
            isKey = true
            val nextType = reader.readBsonType()
            if (nextType == BsonType.END_OF_DOCUMENT) return DECODE_DONE
        } else {
            isKey = false
        }
        return index++
    }
}
