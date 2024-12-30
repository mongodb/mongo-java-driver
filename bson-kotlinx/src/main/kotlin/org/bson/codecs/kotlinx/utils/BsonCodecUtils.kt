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
package org.bson.codecs.kotlinx.utils

import java.util.*
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.json.JsonNamingStrategy
import kotlinx.serialization.modules.SerializersModule
import org.bson.AbstractBsonReader
import org.bson.BsonWriter
import org.bson.codecs.kotlinx.BsonArrayDecoder
import org.bson.codecs.kotlinx.BsonConfiguration
import org.bson.codecs.kotlinx.BsonDecoder
import org.bson.codecs.kotlinx.BsonDecoderImpl
import org.bson.codecs.kotlinx.BsonDocumentDecoder
import org.bson.codecs.kotlinx.BsonEncoder
import org.bson.codecs.kotlinx.BsonEncoderImpl
import org.bson.codecs.kotlinx.BsonMapDecoder
import org.bson.codecs.kotlinx.BsonNamingStrategy
import org.bson.codecs.kotlinx.BsonPolymorphicDecoder
import org.bson.codecs.kotlinx.JsonBsonArrayDecoder
import org.bson.codecs.kotlinx.JsonBsonDecoderImpl
import org.bson.codecs.kotlinx.JsonBsonDocumentDecoder
import org.bson.codecs.kotlinx.JsonBsonEncoder
import org.bson.codecs.kotlinx.JsonBsonMapDecoder
import org.bson.codecs.kotlinx.JsonBsonPolymorphicDecoder

@ExperimentalSerializationApi
internal object BsonCodecUtils {

    @Suppress("SwallowedException")
    private val hasJsonEncoder: Boolean by lazy {
        try {
            Class.forName("kotlinx.serialization.json.JsonEncoder")
            true
        } catch (e: ClassNotFoundException) {
            false
        }
    }

    @Suppress("SwallowedException")
    private val hasJsonDecoder: Boolean by lazy {
        try {
            Class.forName("kotlinx.serialization.json.JsonDecoder")
            true
        } catch (e: ClassNotFoundException) {
            false
        }
    }

    internal fun createBsonEncoder(
        writer: BsonWriter,
        serializersModule: SerializersModule,
        configuration: BsonConfiguration
    ): BsonEncoder {
        return if (hasJsonEncoder) JsonBsonEncoder(writer, serializersModule, configuration)
        else BsonEncoderImpl(writer, serializersModule, configuration)
    }

    internal fun createBsonDecoder(
        reader: AbstractBsonReader,
        serializersModule: SerializersModule,
        configuration: BsonConfiguration
    ): BsonDecoder {
        return if (hasJsonDecoder) JsonBsonDecoderImpl(reader, serializersModule, configuration)
        else BsonDecoderImpl(reader, serializersModule, configuration)
    }

    internal fun createBsonArrayDecoder(
        descriptor: SerialDescriptor,
        reader: AbstractBsonReader,
        serializersModule: SerializersModule,
        configuration: BsonConfiguration
    ): BsonArrayDecoder {
        return if (hasJsonDecoder) JsonBsonArrayDecoder(descriptor, reader, serializersModule, configuration)
        else BsonArrayDecoder(descriptor, reader, serializersModule, configuration)
    }

    internal fun createBsonDocumentDecoder(
        descriptor: SerialDescriptor,
        reader: AbstractBsonReader,
        serializersModule: SerializersModule,
        configuration: BsonConfiguration
    ): BsonDocumentDecoder {
        return if (hasJsonDecoder) JsonBsonDocumentDecoder(descriptor, reader, serializersModule, configuration)
        else BsonDocumentDecoder(descriptor, reader, serializersModule, configuration)
    }

    internal fun createBsonPolymorphicDecoder(
        descriptor: SerialDescriptor,
        reader: AbstractBsonReader,
        serializersModule: SerializersModule,
        configuration: BsonConfiguration
    ): BsonPolymorphicDecoder {
        return if (hasJsonDecoder) JsonBsonPolymorphicDecoder(descriptor, reader, serializersModule, configuration)
        else BsonPolymorphicDecoder(descriptor, reader, serializersModule, configuration)
    }

    internal fun createBsonMapDecoder(
        descriptor: SerialDescriptor,
        reader: AbstractBsonReader,
        serializersModule: SerializersModule,
        configuration: BsonConfiguration
    ): BsonMapDecoder {
        return if (hasJsonDecoder) JsonBsonMapDecoder(descriptor, reader, serializersModule, configuration)
        else BsonMapDecoder(descriptor, reader, serializersModule, configuration)
    }

    internal val String.toSnakeCase
        get() = replace(Regex("([A-Z])"), "_$1").lowercase(Locale.getDefault()).replace(Regex("^_"), "")

    internal val String.toCamelCase
        get() = replace(Regex("_([a-z])")) { it.value[1].uppercaseChar().toString() }

    internal fun BsonNamingStrategy?.toJsonNamingStrategy(): JsonNamingStrategy? {
        return when (this) {
            BsonNamingStrategy.SNAKE_CASE -> JsonNamingStrategy.SnakeCase
            else -> null
        }
    }
}
