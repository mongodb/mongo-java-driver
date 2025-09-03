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
package org.bson.codecs.kotlin

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import kotlin.reflect.KClass
import kotlin.reflect.KClassifier
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty1
import kotlin.reflect.KTypeParameter
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.findAnnotations
import kotlin.reflect.full.hasAnnotation
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.javaType
import kotlin.reflect.jvm.jvmErasure
import org.bson.BsonReader
import org.bson.BsonType
import org.bson.BsonWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.RepresentationConfigurable
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.pojo.annotations.BsonCreator
import org.bson.codecs.pojo.annotations.BsonDiscriminator
import org.bson.codecs.pojo.annotations.BsonExtraElements
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonIgnore
import org.bson.codecs.pojo.annotations.BsonProperty
import org.bson.codecs.pojo.annotations.BsonRepresentation
import org.bson.diagnostics.Loggers

internal data class DataClassCodec<T : Any>(
    private val kClass: KClass<T>,
    private val primaryConstructor: KFunction<T>,
    private val propertyModels: List<PropertyModel>,
) : Codec<T> {

    private val fieldNamePropertyModelMap = propertyModels.associateBy { it.fieldName }
    private val propertyModelId: PropertyModel? = fieldNamePropertyModelMap[idFieldName]

    data class PropertyModel(val param: KParameter, val fieldName: String, val codec: Codec<Any>)

    override fun encode(writer: BsonWriter, value: T, encoderContext: EncoderContext) {
        writer.writeStartDocument()
        if (propertyModelId != null) {
            encodeProperty(propertyModelId, value, writer, encoderContext)
        }
        propertyModels
            .filter { it != propertyModelId }
            .forEach { propertyModel -> encodeProperty(propertyModel, value, writer, encoderContext) }
        writer.writeEndDocument()
    }

    override fun getEncoderClass(): Class<T> = kClass.java

    @Suppress("TooGenericExceptionCaught")
    override fun decode(reader: BsonReader, decoderContext: DecoderContext): T {
        val args: MutableMap<KParameter, Any?> = mutableMapOf()
        fieldNamePropertyModelMap.values.forEach { args[it.param] = null }

        reader.readStartDocument()
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            val fieldName = reader.readName()
            val propertyModel = fieldNamePropertyModelMap[fieldName]
            if (propertyModel == null) {
                reader.skipValue()
                if (logger.isTraceEnabled) {
                    logger.trace("Found property not present in the DataClass: $fieldName")
                }
            } else if (propertyModel.param.type.isMarkedNullable && reader.currentBsonType == BsonType.NULL) {
                reader.readNull()
            } else {
                try {
                    args[propertyModel.param] = decoderContext.decodeWithChildContext(propertyModel.codec, reader)
                } catch (e: Exception) {
                    throw CodecConfigurationException(
                        "Unable to decode $fieldName for ${kClass.simpleName} data class.", e)
                }
            }
        }
        reader.readEndDocument()

        try {
            return primaryConstructor.callBy(args)
        } catch (e: Exception) {
            throw CodecConfigurationException(
                "Unable to invoke primary constructor of ${kClass.simpleName} data class", e)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> encodeProperty(
        propertyModel: PropertyModel,
        value: T,
        writer: BsonWriter,
        encoderContext: EncoderContext
    ) {
        value::class
            .members
            .firstOrNull { it.name == propertyModel.param.name }
            ?.let {
                val propertyValue = (it as KProperty1<Any, *>).get(value)
                propertyValue?.let { pValue ->
                    writer.writeName(propertyModel.fieldName)
                    encoderContext.encodeWithChildContext(propertyModel.codec, writer, pValue)
                }
            }
    }

    companion object {

        internal val logger = Loggers.getLogger("DataClassCodec")
        private const val idFieldName = "_id"

        internal fun <R : Any> create(
            kClass: KClass<R>,
            codecRegistry: CodecRegistry,
            types: List<Type> = emptyList()
        ): Codec<R>? {
            return if (kClass.isData) {
                validateAnnotations(kClass)
                val primaryConstructor =
                    kClass.primaryConstructor ?: throw CodecConfigurationException("No primary constructor for $kClass")
                val typeMap =
                    types
                        .mapIndexed { i, k -> primaryConstructor.typeParameters[i].createType().classifier!! to k }
                        .toMap()

                val propertyModels =
                    primaryConstructor.parameters.map { kParameter ->
                        PropertyModel(
                            kParameter, computeFieldName(kParameter), getCodec(kParameter, typeMap, codecRegistry))
                    }
                return DataClassCodec(kClass, primaryConstructor, propertyModels)
            } else {
                null
            }
        }

        private fun <R : Any> validateAnnotations(kClass: KClass<R>) {
            codecConfigurationRequires(kClass.findAnnotation<BsonDiscriminator>() == null) {
                """Annotation 'BsonDiscriminator' is not supported on kotlin data classes,
                    | but found on ${kClass.simpleName}."""
                    .trimMargin()
            }

            codecConfigurationRequires(kClass.constructors.all { it.findAnnotations<BsonCreator>().isEmpty() }) {
                """Annotation 'BsonCreator' is not supported on kotlin data classes,
                    | but found in ${kClass.simpleName}."""
                    .trimMargin()
            }

            kClass.primaryConstructor?.parameters?.map { param ->
                codecConfigurationRequires(param.findAnnotations<BsonIgnore>().isEmpty()) {
                    """Annotation 'BsonIgnore' is not supported in kotlin data classes,
                        | found on the parameter for ${param.name}."""
                        .trimMargin()
                }
                codecConfigurationRequires(param.findAnnotations<BsonExtraElements>().isEmpty()) {
                    """Annotation 'BsonExtraElements' is not supported in kotlin data classes,
                        | found on the parameter for ${param.name}."""
                        .trimMargin()
                }
            }
        }

        private fun computeFieldName(parameter: KParameter): String {
            return if (parameter.hasAnnotation<BsonId>()) {
                idFieldName
            } else {
                parameter.findAnnotation<BsonProperty>()?.value ?: requireNotNull(parameter.name)
            }
        }

        @Suppress("UNCHECKED_CAST")
        private fun getCodec(
            kParameter: KParameter,
            typeMap: Map<KClassifier, Type>,
            codecRegistry: CodecRegistry
        ): Codec<Any> {
            return when (kParameter.type.classifier) {
                is KClass<*> -> {
                    codecRegistry.getCodec(
                        kParameter,
                        (kParameter.type.classifier as KClass<Any>).javaObjectType,
                        kParameter.type.arguments
                            .mapNotNull { typeMap[it.type?.classifier] ?: computeJavaType(it) }
                            .toList())
                }
                is KTypeParameter -> {
                    when (val pType = typeMap[kParameter.type.classifier] ?: kParameter.type.javaType) {
                        is Class<*> ->
                            codecRegistry.getCodec(kParameter, (pType as Class<Any>).kotlin.java, emptyList())
                        is ParameterizedType ->
                            codecRegistry.getCodec(
                                kParameter,
                                (pType.rawType as Class<Any>).kotlin.javaObjectType,
                                pType.actualTypeArguments.toList())
                        else -> null
                    }
                }
                else -> null
            }
                ?: throw CodecConfigurationException(
                    "Could not find codec for ${kParameter.name} with type ${kParameter.type}")
        }

        private fun computeJavaType(kTypeProjection: KTypeProjection): Type? {
            val javaType: Type = kTypeProjection.type?.javaType!!
            return if (javaType == Any::class.java) {
                kTypeProjection.type?.jvmErasure?.javaObjectType
            } else javaType
        }

        @Suppress("UNCHECKED_CAST")
        private fun CodecRegistry.getCodec(kParameter: KParameter, clazz: Class<Any>, types: List<Type>): Codec<Any> {
            val codec =
                if (clazz.isArray) {
                    ArrayCodec.create(clazz.kotlin, types, this)
                } else if (types.isEmpty()) {
                    this.get(clazz)
                } else {
                    this.get(clazz, types)
                }

            return kParameter.findAnnotation<BsonRepresentation>()?.let {
                if (codec !is RepresentationConfigurable<*>) {
                    throw CodecConfigurationException(
                        "Codec for `${kParameter.name}` must implement RepresentationConfigurable" +
                            " to supportBsonRepresentation")
                }
                codec.withRepresentation(it.value) as Codec<Any>
            }
                ?: codec
        }

        private fun codecConfigurationRequires(value: Boolean, lazyMessage: () -> String) {
            if (!value) {
                throw CodecConfigurationException(lazyMessage.invoke())
            }
        }
    }
}
