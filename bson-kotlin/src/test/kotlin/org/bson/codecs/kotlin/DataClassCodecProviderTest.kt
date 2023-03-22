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

import com.mongodb.MongoClientSettings
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.kotlin.samples.DataClassEmbedded
import org.bson.codecs.kotlin.samples.DataClassParameterized
import org.bson.codecs.kotlin.samples.DataClassWithParameterizedDataClass
import org.bson.codecs.kotlin.samples.DataClassWithSimpleValues
import org.bson.conversions.Bson
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class DataClassCodecProviderTest {

    @Test
    fun shouldReturnNullForNonDataClass() {
        assertNull(DataClassCodecProvider().get(String::class.java, Bson.DEFAULT_CODEC_REGISTRY))
    }

    @Test
    fun shouldReturnDataClassCodecForDataClass() {
        val provider = DataClassCodecProvider()
        val codec = provider.get(DataClassWithSimpleValues::class.java, Bson.DEFAULT_CODEC_REGISTRY)

        assertNotNull(codec)
        assertTrue { codec is DataClassCodec }
        assertEquals(DataClassWithSimpleValues::class.java, codec.encoderClass)
    }

    @Test
    fun shouldReturnRawDataClassCodecForParameterizedDataClass() {
        val provider = DataClassCodecProvider()
        val codec = provider.get(DataClassParameterized::class.java, Bson.DEFAULT_CODEC_REGISTRY)

        assertNotNull(codec)
        assertTrue { codec is DataClassCodec.Companion.RawDataClassCodec }
        assertEquals(DataClassParameterized::class.java, codec.encoderClass)

        assertThrows<CodecConfigurationException> {
            val writer = BsonDocumentWriter(BsonDocument())
            val dataClass =
                DataClassWithParameterizedDataClass(
                    "myId", DataClassParameterized(2.0, "myString", listOf(DataClassEmbedded("embedded1"))))
            codec.encode(writer, dataClass.parameterizedDataClass, EncoderContext.builder().build())
        }

        assertThrows<CodecConfigurationException> {
            val value =
                BsonDocument.parse(
                    """{"number": 2.0, "string": "myString", "parameterizedList": [{"name": "embedded1"}]}""")
            codec.decode(BsonDocumentReader(value), DecoderContext.builder().build())
        }
    }

    @Test
    fun shouldReturnDataClassCodecUsingDefaultRegistry() {
        val codec = MongoClientSettings.getDefaultCodecRegistry().get(DataClassWithSimpleValues::class.java)

        assertNotNull(codec)
        assertTrue { codec is DataClassCodec }
        assertEquals(DataClassWithSimpleValues::class.java, codec.encoderClass)
    }
}
