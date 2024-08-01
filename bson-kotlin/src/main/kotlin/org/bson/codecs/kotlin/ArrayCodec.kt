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
import org.bson.BsonReader
import org.bson.BsonType
import org.bson.BsonWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecRegistry

@Suppress("UNCHECKED_CAST")
internal data class ArrayCodec<R : Any, V>(private val kClass: KClass<R>, private val codec: Codec<V?>) : Codec<R> {

    companion object {
        internal fun <R : Any> create(
            kClass: KClass<R>,
            typeArguments: List<Type>,
            codecRegistry: CodecRegistry
        ): Codec<R> {
            assert(kClass.javaObjectType.isArray) { "$kClass must be an array type" }
            val (valueClass, nestedTypes) =
                if (typeArguments.isEmpty()) {
                    Pair(kClass.java.componentType.kotlin.javaObjectType as Class<Any>, emptyList())
                } else {
                    // Unroll the actual class and any type arguments
                    when (val pType = typeArguments[0]) {
                        is Class<*> -> Pair(pType as Class<Any>, emptyList())
                        is ParameterizedType -> Pair(pType.rawType as Class<Any>, pType.actualTypeArguments.toList())
                        else -> Pair(Object::class.java as Class<Any>, emptyList())
                    }
                }
            val codec =
                if (nestedTypes.isEmpty()) codecRegistry.get(valueClass) else codecRegistry.get(valueClass, nestedTypes)
            return ArrayCodec(kClass, codec)
        }
    }

    private val isPrimitiveArray = kClass.java.componentType != kClass.java.componentType.kotlin.javaObjectType

    override fun encode(writer: BsonWriter, arrayValue: R, encoderContext: EncoderContext) {
        writer.writeStartArray()

        boxed(arrayValue).forEach {
            if (it == null) writer.writeNull() else encoderContext.encodeWithChildContext(codec, writer, it)
        }

        writer.writeEndArray()
    }

    override fun getEncoderClass(): Class<R> = kClass.java

    override fun decode(reader: BsonReader, decoderContext: DecoderContext): R {
        reader.readStartArray()
        val data = ArrayList<V?>()
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            if (reader.currentBsonType == BsonType.NULL) {
                reader.readNull()
                data.add(null)
            } else {
                data.add(decoderContext.decodeWithChildContext(codec, reader))
            }
        }
        reader.readEndArray()
        return unboxed(data)
    }

    fun boxed(arrayValue: R): Iterable<V?> {
        val boxedValue =
            if (!isPrimitiveArray) {
                (arrayValue as Array<V?>).asIterable()
            } else if (arrayValue is BooleanArray) {
                arrayValue.asIterable()
            } else if (arrayValue is ByteArray) {
                arrayValue.asIterable()
            } else if (arrayValue is CharArray) {
                arrayValue.asIterable()
            } else if (arrayValue is DoubleArray) {
                arrayValue.asIterable()
            } else if (arrayValue is FloatArray) {
                arrayValue.asIterable()
            } else if (arrayValue is IntArray) {
                arrayValue.asIterable()
            } else if (arrayValue is LongArray) {
                arrayValue.asIterable()
            } else if (arrayValue is ShortArray) {
                arrayValue.asIterable()
            } else {
                throw IllegalArgumentException("Unsupported array type ${arrayValue.javaClass}")
            }
        return boxedValue as Iterable<V?>
    }

    private fun unboxed(data: ArrayList<V?>): R {
        return when (kClass) {
            BooleanArray::class -> (data as ArrayList<Boolean>).toBooleanArray() as R
            ByteArray::class -> (data as ArrayList<Byte>).toByteArray() as R
            CharArray::class -> (data as ArrayList<Char>).toCharArray() as R
            DoubleArray::class -> (data as ArrayList<Double>).toDoubleArray() as R
            FloatArray::class -> (data as ArrayList<Float>).toFloatArray() as R
            IntArray::class -> (data as ArrayList<Int>).toIntArray() as R
            LongArray::class -> (data as ArrayList<Long>).toLongArray() as R
            ShortArray::class -> (data as ArrayList<Short>).toShortArray() as R
            else -> data.toArray(arrayOfNulls(data.size)) as R
        }
    }

    private fun arrayOfNulls(size: Int): Array<V?> {
        return java.lang.reflect.Array.newInstance(codec.encoderClass, size) as Array<V?>
    }
}
