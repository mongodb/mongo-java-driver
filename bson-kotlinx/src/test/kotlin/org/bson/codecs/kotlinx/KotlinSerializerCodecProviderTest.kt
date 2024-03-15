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

import com.mongodb.MongoClientSettings
import org.bson.codecs.DecoderContext
import org.bson.codecs.kotlinx.samples.DataClassParameterized
import org.bson.codecs.kotlinx.samples.DataClassSealedInterface
import org.bson.codecs.kotlinx.samples.DataClassWithSimpleValues
import org.bson.codecs.kotlinx.samples.SealedInterface
import org.bson.conversions.Bson
import org.bson.json.JsonReader
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KotlinSerializerCodecProviderTest {

    data class NotMarkedSerializable(val t: String)

    @Test
    fun shouldReturnNullForNonSerializableClass() {
        assertNull(KotlinSerializerCodecProvider().get(NotMarkedSerializable::class.java, Bson.DEFAULT_CODEC_REGISTRY))
        assertNull(KotlinSerializerCodecProvider().get(DoubleArray::class.java, Bson.DEFAULT_CODEC_REGISTRY))
        assertNull(KotlinSerializerCodecProvider().get(CharSequence::class.java, Bson.DEFAULT_CODEC_REGISTRY))
    }

    @Test
    fun shouldReturnKotlinSerializerCodecForDataClass() {
        val provider = KotlinSerializerCodecProvider()
        val codec = provider.get(DataClassWithSimpleValues::class.java, Bson.DEFAULT_CODEC_REGISTRY)

        assertNotNull(codec)
        assertTrue { codec is KotlinSerializerCodec }
        assertEquals(DataClassWithSimpleValues::class.java, codec.encoderClass)
    }

    @Test
    fun shouldReturnNullFoRawParameterizedDataClass() {
        val codec = KotlinSerializerCodecProvider().get(DataClassParameterized::class.java, Bson.DEFAULT_CODEC_REGISTRY)
        assertNull(codec)
    }

    @Test
    fun shouldReturnKotlinSerializerCodecUsingDefaultRegistry() {
        val codec = MongoClientSettings.getDefaultCodecRegistry().get(DataClassWithSimpleValues::class.java)

        assertNotNull(codec)
        assertTrue { codec is KotlinSerializerCodec }
        assertEquals(DataClassWithSimpleValues::class.java, codec.encoderClass)
    }

    @Test
    fun testDataClassWithSimpleValuesFieldOrdering() {
        val codec = MongoClientSettings.getDefaultCodecRegistry().get(DataClassWithSimpleValues::class.java)
        val expected = DataClassWithSimpleValues('c', 0, 1, 22, 42L, 4.0f, 4.2, true, "String")

        val numberLong = "\$numberLong"
        val actual =
            codec.decode(
                JsonReader(
                    """{"boolean": true, "byte": 0, "char": "c", "double": 4.2, "float": 4.0, "int": 22,
                        |"long": {"$numberLong": "42"}, "short": 1, "string": "String"}"""
                        .trimMargin()),
                DecoderContext.builder().build())

        assertEquals(expected, actual)
    }

    @Test
    fun testDataClassSealedFieldOrdering() {
        val codec = MongoClientSettings.getDefaultCodecRegistry().get(SealedInterface::class.java)

        val objectId = ObjectId("111111111111111111111111")
        val oid = "\$oid"
        val expected = DataClassSealedInterface(objectId, "string")
        val actual =
            codec.decode(
                JsonReader(
                    """{"name": "string", "_id": {$oid: "${objectId.toHexString()}"},
                            |"_t": "org.bson.codecs.kotlinx.samples.DataClassSealedInterface"}"""
                        .trimMargin()),
                DecoderContext.builder().build())

        assertEquals(expected, actual)
    }
}
