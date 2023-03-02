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

import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.bson.codecs.kotlin.DataClassCodec.Companion.createDataClassCodec
import org.bson.codecs.kotlin.samples.DataClass
import org.bson.codecs.kotlin.samples.DataClassEmbedded
import org.bson.codecs.kotlin.samples.DataClassListOfDataClasses
import org.bson.codecs.kotlin.samples.DataClassListOfListOfDataClasses
import org.bson.codecs.kotlin.samples.DataClassMapOfDataClasses
import org.bson.codecs.kotlin.samples.DataClassMapOfListOfDataClasses
import org.bson.codecs.kotlin.samples.DataClassNestedParameterizedTypes
import org.bson.codecs.kotlin.samples.DataClassParameterized
import org.bson.codecs.kotlin.samples.DataClassSelfReferential
import org.bson.codecs.kotlin.samples.DataClassWithAnnotations
import org.bson.codecs.kotlin.samples.DataClassWithBsonConstructor
import org.bson.codecs.kotlin.samples.DataClassWithBsonDiscriminator
import org.bson.codecs.kotlin.samples.DataClassWithBsonExtraElements
import org.bson.codecs.kotlin.samples.DataClassWithBsonIgnore
import org.bson.codecs.kotlin.samples.DataClassWithDefaults
import org.bson.codecs.kotlin.samples.DataClassWithEmbedded
import org.bson.codecs.kotlin.samples.DataClassWithFailingInit
import org.bson.codecs.kotlin.samples.DataClassWithInvalidRepresentation
import org.bson.codecs.kotlin.samples.DataClassWithNestedParameterized
import org.bson.codecs.kotlin.samples.DataClassWithNestedParameterizedDataClass
import org.bson.codecs.kotlin.samples.DataClassWithNulls
import org.bson.codecs.kotlin.samples.DataClassWithPair
import org.bson.codecs.kotlin.samples.DataClassWithParameterizedDataClass
import org.bson.codecs.kotlin.samples.DataClassWithTriple
import org.bson.conversions.Bson
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class DataClassCodecTest {

    @Test
    fun testDataClass() {
        val expected = """{"_id": "myId", "name": "Felix", "age": 14, "hobbies": ["rugby", "weights"]}"""
        val dataClass = DataClass("myId", "Felix", 14, listOf("rugby", "weights"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithEmbedded() {
        val expected = """{"_id": "myId", "embedded": {"name": "embedded1"}}"""
        val dataClass = DataClassWithEmbedded("myId", DataClassEmbedded("embedded1"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithAnnotations() {
        val oid = "\$oid"
        val expected =
            """{"_id": {"$oid": "111111111111111111111111"},
                |"nom": "Felix", "age": 14, "hobbies": ["rugby", "weights"]}"""
                .trimMargin()
        val dataClass = DataClassWithAnnotations("111111111111111111111111", "Felix", 14, listOf("rugby", "weights"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassListOfDataClasses() {
        val expected = """{"_id": "myId", "nested": [{"name": "embedded1"}, {"name": "embedded2"}]}"""
        val dataClass =
            DataClassListOfDataClasses("myId", listOf(DataClassEmbedded("embedded1"), DataClassEmbedded("embedded2")))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassListOfListOfDataClasses() {
        val expected = """{"_id": "myId", "nested": [[{"name": "embedded1"}], [{"name": "embedded2"}]]}"""
        val dataClass =
            DataClassListOfListOfDataClasses(
                "myId", listOf(listOf(DataClassEmbedded("embedded1")), listOf(DataClassEmbedded("embedded2"))))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassMapOfDataClasses() {
        val expected =
            """{"_id": "myId", "nested": {"first": {"name": "embedded1"}, "second": {"name": "embedded2"}}}"""
        val dataClass =
            DataClassMapOfDataClasses(
                "myId", mapOf("first" to DataClassEmbedded("embedded1"), "second" to DataClassEmbedded("embedded2")))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassMapOfListOfDataClasses() {
        val expected =
            """{"_id": "myId", "nested": {"first": [{"name": "embedded1"}], "second": [{"name": "embedded2"}]}}"""
        val dataClass =
            DataClassMapOfListOfDataClasses(
                "myId",
                mapOf(
                    "first" to listOf(DataClassEmbedded("embedded1")),
                    "second" to listOf(DataClassEmbedded("embedded2"))))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithNulls() {
        val expected = """{"name": "Felix", "hobbies": ["rugby", "weights"]}"""
        val dataClass = DataClassWithNulls(null, "Felix", null, listOf("rugby", "weights"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithDefaults() {
        val expected = """{"_id": "myId", "name": "Arthur Dent", "age": 42, "hobbies": ["computers", "databases"]}"""
        val dataClass = DataClassWithDefaults("myId", "Arthur Dent")

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testHandlesExtraData() {
        val expected =
            """{"_id": "myId", "extra1": "extraField", "name": "Felix",  "extra2": "extraField", "age": 14,
            | "extra3": "extraField", "hobbies": ["rugby", "weights"],  "extra4": "extraField"}"""
                .trimMargin()
        val dataClass = DataClass("myId", "Felix", 14, listOf("rugby", "weights"))

        assertDecodesTo(dataClass, BsonDocument.parse(expected))
    }

    @Test
    fun testDataClassSelfReferential() {
        val expected =
            """{"_id": "myId", "name": "tree",
            | "left": {"name": "L", "left": {"name": "LL"}, "right": {"name": "LR"}},
            | "right": {"name": "R",
            |           "left": {"name": "RL",
            |             "left": {"name": "RLL"},
            |             "right": {"name": "RLR"}},
            |           "right": {"name": "RR"}}
            |}"""
                .trimMargin()
        val dataClass =
            DataClassSelfReferential(
                "tree",
                DataClassSelfReferential("L", DataClassSelfReferential("LL"), DataClassSelfReferential("LR")),
                DataClassSelfReferential(
                    "R",
                    DataClassSelfReferential("RL", DataClassSelfReferential("RLL"), DataClassSelfReferential("RLR")),
                    DataClassSelfReferential("RR")),
                id = "myId")

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithParameterizedDataClass() {
        val expected =
            """{"_id": "myId",
            | "parameterizedDataClass": {"number": 2.0, "string": "myString",
            |                            "parameterizedList": [{"name": "embedded1"}]}
            |}"""
                .trimMargin()
        val dataClass =
            DataClassWithParameterizedDataClass(
                "myId", DataClassParameterized(2.0, "myString", listOf(DataClassEmbedded("embedded1"))))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithNestedParameterizedDataClass() {
        val expected =
            """{"_id": "myId",
            |"nestedParameterized": {
            |  "parameterizedDataClass":
            |  {"number": 4.2, "string": "myString", "parameterizedList": [{"name": "embedded1"}]},
            |  "other": "myOtherString"
            | }
            |}"""
                .trimMargin()
        val dataClass =
            DataClassWithNestedParameterizedDataClass(
                "myId",
                DataClassWithNestedParameterized(
                    DataClassParameterized(4.2, "myString", listOf(DataClassEmbedded("embedded1"))), "myOtherString"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithPair() {
        val expected = """{"pair": {"first": "a", "second": 1}}"""
        val dataClass = DataClassWithPair("a" to 1)

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithTriple() {
        val expected = """{"triple": {"first": "a", "second": 1, "third": 2.1}}"""
        val dataClass = DataClassWithTriple(Triple("a", 1, 2.1))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassNestedParameterizedTypes() {

        val expected =
            """{
            |"triple": {
            |  "first": "0",
            |  "second": {"first": 1, "second": {"first": 1.2, "second": {"first": "1.3", "second": 1.3}}},
            |  "third":  {"first": 2, "second": {"first": 2.1, "second": "two dot two"},
            |             "third": {"first": "3.1", "second": {"first": 3.2, "second": "three dot two" },
            |                       "third": 3.3 }}
            | }
            |}"""
                .trimMargin()
        val dataClass =
            DataClassNestedParameterizedTypes(
                Triple(
                    "0",
                    Pair(1, Pair(1.2, Pair("1.3", 1.3))),
                    Triple(2, Pair(2.1, "two dot two"), Triple("3.1", Pair(3.2, "three dot two"), 3.3))))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataFailures() {
        assertThrows<CodecConfigurationException>("Missing data") {
            val codec: Codec<DataClass> = createDataClassCodec(DataClass::class, registry())
            codec.decode(BsonDocumentReader(BsonDocument()), DecoderContext.builder().build())
        }

        assertThrows<CodecConfigurationException>("Invalid types") {
            val data =
                BsonDocument.parse("""{"_id": "myId", "name": "Imogen", "age": "16", "hobbies": ["rugby", "gym"]}""")
            val codec: Codec<DataClass> = createDataClassCodec(DataClass::class, registry())
            codec.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }

        assertThrows<CodecConfigurationException>("Invalid complex types") {
            val data = BsonDocument.parse("""{"_id": "myId", "embedded": 123}""")
            val codec: Codec<DataClassWithEmbedded> = createDataClassCodec(DataClassWithEmbedded::class, registry())
            codec.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }

        assertThrows<CodecConfigurationException>("Failing init") {
            val data = BsonDocument.parse("""{"_id": "myId"}""")
            val codec: DataClassCodec<DataClassWithFailingInit> =
                createDataClassCodec(DataClassWithFailingInit::class, registry())
            codec.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }
    }

    @Test
    fun testInvalidAnnotations() {
        assertThrows<CodecConfigurationException> {
            createDataClassCodec(DataClassWithBsonDiscriminator::class, Bson.DEFAULT_CODEC_REGISTRY)
        }
        assertThrows<CodecConfigurationException> {
            createDataClassCodec(DataClassWithBsonConstructor::class, Bson.DEFAULT_CODEC_REGISTRY)
        }
        assertThrows<CodecConfigurationException> {
            createDataClassCodec(DataClassWithBsonIgnore::class, Bson.DEFAULT_CODEC_REGISTRY)
        }
        assertThrows<CodecConfigurationException> {
            createDataClassCodec(DataClassWithBsonExtraElements::class, Bson.DEFAULT_CODEC_REGISTRY)
        }
        assertThrows<CodecConfigurationException> {
            createDataClassCodec(DataClassWithInvalidRepresentation::class, Bson.DEFAULT_CODEC_REGISTRY)
        }
    }

    private fun <T : Any> assertRoundTrips(expected: String, value: T) {
        assertDecodesTo(value, assertEncodesTo(expected, value))
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> assertEncodesTo(json: String, value: T): BsonDocument {
        val expected = BsonDocument.parse(json)
        val codec: DataClassCodec<T> = createDataClassCodec(value::class, registry()) as DataClassCodec<T>
        val document = BsonDocument()
        val writer = BsonDocumentWriter(document)

        codec.encode(writer, value, EncoderContext.builder().build())
        assertEquals(expected, document)
        if (expected.contains("_id")) {
            assertEquals("_id", document.firstKey)
        }
        return document
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> assertDecodesTo(expected: T, actual: BsonDocument) {
        val codec: DataClassCodec<T> = createDataClassCodec(expected::class, registry()) as DataClassCodec<T>
        val decoded: T = codec.decode(BsonDocumentReader(actual), DecoderContext.builder().build())

        assertEquals(expected, decoded)
    }

    private fun registry() = fromProviders(DataClassCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY)
}
