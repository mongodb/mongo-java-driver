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

import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.findAnnotations
import kotlin.reflect.full.primaryConstructor
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.bson.AbstractBsonReader
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.pojo.annotations.BsonCreator
import org.bson.codecs.pojo.annotations.BsonDiscriminator
import org.bson.codecs.pojo.annotations.BsonExtraElements
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonIgnore
import org.bson.codecs.pojo.annotations.BsonProperty
import org.bson.codecs.pojo.annotations.BsonRepresentation

/**
 * The Kotlin serializer codec which utilizes the kotlinx serialization module.
 *
 * Use the [create] method to create the codec
 */
@OptIn(ExperimentalSerializationApi::class, InternalSerializationApi::class)
public class KotlinSerializerCodec<T : Any>
private constructor(
    private val kClass: KClass<T>,
    private val serializer: KSerializer<T>,
    private val serializersModule: SerializersModule,
    private val bsonConfiguration: BsonConfiguration
) : Codec<T> {

    /** KotlinSerializerCodec companion object */
    public companion object {

        /**
         * Creates a `Codec<T>` for the kClass or returns null if there is no serializer available.
         *
         * @param T The codec type
         * @param serializersModule the serializiers module to use
         * @param bsonConfiguration the bson configuration for serializing
         * @return the codec
         */
        public inline fun <reified T : Any> create(
            serializersModule: SerializersModule = defaultSerializersModule,
            bsonConfiguration: BsonConfiguration = BsonConfiguration()
        ): Codec<T>? = create(T::class, serializersModule, bsonConfiguration)

        /**
         * Creates a `Codec<T>` for the kClass or returns null if there is no serializer available.
         *
         * @param T The codec type
         * @param kClass the KClass for the codec
         * @param serializersModule the serializiers module to use
         * @param bsonConfiguration the bson configuration for serializing
         * @return the codec
         */
        @Suppress("SwallowedException")
        public fun <T : Any> create(
            kClass: KClass<T>,
            serializersModule: SerializersModule = defaultSerializersModule,
            bsonConfiguration: BsonConfiguration = BsonConfiguration()
        ): Codec<T>? {
            return try {
                create(kClass, kClass.serializer(), serializersModule, bsonConfiguration)
            } catch (exception: SerializationException) {
                null
            }
        }

        /**
         * Creates a `Codec<T>` for the kClass using the supplied serializer
         *
         * @param T The codec type
         * @param kClass the KClass for the codec
         * @param serializer the KSerializer to use
         * @param serializersModule the serializiers module to use
         * @param bsonConfiguration the bson configuration for serializing
         * @return the codec
         */
        public fun <T : Any> create(
            kClass: KClass<T>,
            serializer: KSerializer<T>,
            serializersModule: SerializersModule,
            bsonConfiguration: BsonConfiguration
        ): Codec<T> {
            validateAnnotations(kClass)
            return KotlinSerializerCodec(kClass, serializer, serializersModule, bsonConfiguration)
        }

        private fun <R : Any> validateAnnotations(kClass: KClass<R>) {
            codecConfigurationRequires(kClass.findAnnotation<BsonDiscriminator>() == null) {
                """Annotation 'BsonDiscriminator' is not supported with kotlin serialization,
                    | but found on ${kClass.simpleName}. Use `BsonConfiguration` with `KotlinSerializerCodec.create`
                    | to configure a discriminator."""
                    .trimMargin()
            }

            codecConfigurationRequires(kClass.constructors.all { it.findAnnotations<BsonCreator>().isEmpty() }) {
                """Annotation 'BsonCreator' is not supported with kotlin serialization,
                    | but found in ${kClass.simpleName}."""
                    .trimMargin()
            }

            kClass.primaryConstructor?.parameters?.map { param ->
                codecConfigurationRequires(param.findAnnotations<BsonId>().isEmpty()) {
                    """Annotation 'BsonId' is not supported with kotlin serialization,
                        | found on the parameter for ${param.name}. Use `@SerialName("_id")` instead."""
                        .trimMargin()
                }

                codecConfigurationRequires(param.findAnnotations<BsonProperty>().isEmpty()) {
                    """Annotation 'BsonProperty' is not supported with kotlin serialization,
                        | found on the parameter for ${param.name}. Use `@SerialName` instead."""
                        .trimMargin()
                }

                codecConfigurationRequires(param.findAnnotations<BsonIgnore>().isEmpty()) {
                    """Annotation 'BsonIgnore' is not supported with kotlinx serialization,
                        | found on the parameter for ${param.name}. Use `@Transient` annotation to ignore a property."""
                        .trimMargin()
                }

                codecConfigurationRequires(param.findAnnotations<BsonExtraElements>().isEmpty()) {
                    """Annotation 'BsonExtraElements' is not supported with kotlinx serialization,
                        | found on the parameter for ${param.name}."""
                        .trimMargin()
                }

                codecConfigurationRequires(param.findAnnotations<BsonRepresentation>().isEmpty()) {
                    """Annotation 'BsonRepresentation' is not supported with kotlinx serialization,
                        | found on the parameter for ${param.name}."""
                        .trimMargin()
                }
            }
        }
        private fun codecConfigurationRequires(value: Boolean, lazyMessage: () -> String) {
            if (!value) {
                throw CodecConfigurationException(lazyMessage.invoke())
            }
        }
    }

    override fun encode(writer: BsonWriter, value: T, encoderContext: EncoderContext) {
        serializer.serialize(DefaultBsonEncoder(writer, serializersModule, bsonConfiguration), value)
    }

    override fun getEncoderClass(): Class<T> = kClass.java

    override fun decode(reader: BsonReader, decoderContext: DecoderContext): T {
        require(reader is AbstractBsonReader)
        return serializer.deserialize(DefaultBsonDecoder(reader, serializersModule, bsonConfiguration))
    }
}
