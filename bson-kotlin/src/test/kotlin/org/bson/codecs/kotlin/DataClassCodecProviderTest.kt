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
import kotlin.time.Duration
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.CodecRegistries.fromCodecs
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.bson.codecs.configuration.CodecRegistries.fromRegistries
import org.bson.codecs.kotlin.samples.DataClassParameterized
import org.bson.codecs.kotlin.samples.DataClassWithJVMErasure
import org.bson.codecs.kotlin.samples.DataClassWithSimpleValues
import org.bson.conversions.Bson
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
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
    fun shouldRequireTypeArgumentsForDataClassParameterized() {
        assertThrows<CodecConfigurationException> {
            DataClassCodecProvider().get(DataClassParameterized::class.java, Bson.DEFAULT_CODEC_REGISTRY)
        }
    }

    @Test
    fun shouldReturnDataClassCodecUsingDefaultRegistry() {
        val codec = MongoClientSettings.getDefaultCodecRegistry().get(DataClassWithSimpleValues::class.java)

        assertNotNull(codec)
        assertTrue { codec is DataClassCodec }
        assertEquals(DataClassWithSimpleValues::class.java, codec.encoderClass)
    }

    @Test
    fun shouldBeAbleHandleDataClassWithJVMErasure() {

        class DurationCodec : Codec<Duration> {
            override fun encode(writer: BsonWriter, value: Duration, encoderContext: EncoderContext) = TODO()
            override fun getEncoderClass(): Class<Duration> = Duration::class.java
            override fun decode(reader: BsonReader, decoderContext: DecoderContext): Duration = TODO()
        }

        val registry =
            fromRegistries(
                fromCodecs(DurationCodec()), fromProviders(DataClassCodecProvider()), Bson.DEFAULT_CODEC_REGISTRY)

        val codec = assertDoesNotThrow { registry.get(DataClassWithJVMErasure::class.java) }
        assertNotNull(codec)
        assertTrue { codec is DataClassCodec }
        assertEquals(DataClassWithJVMErasure::class.java, codec.encoderClass)
    }
}
