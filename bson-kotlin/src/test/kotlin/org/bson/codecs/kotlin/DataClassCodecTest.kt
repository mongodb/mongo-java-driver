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
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.bson.codecs.kotlin.samples.DataClassEmbedded
import org.bson.codecs.kotlin.samples.DataClassListOfDataClasses
import org.bson.codecs.kotlin.samples.DataClassListOfListOfDataClasses
import org.bson.codecs.kotlin.samples.DataClassListOfSealed
import org.bson.codecs.kotlin.samples.DataClassMapOfDataClasses
import org.bson.codecs.kotlin.samples.DataClassMapOfListOfDataClasses
import org.bson.codecs.kotlin.samples.DataClassNestedParameterizedTypes
import org.bson.codecs.kotlin.samples.DataClassParameterized
import org.bson.codecs.kotlin.samples.DataClassSealedA
import org.bson.codecs.kotlin.samples.DataClassSealedB
import org.bson.codecs.kotlin.samples.DataClassSealedC
import org.bson.codecs.kotlin.samples.DataClassSelfReferential
import org.bson.codecs.kotlin.samples.DataClassWithBooleanMapKey
import org.bson.codecs.kotlin.samples.DataClassWithBsonConstructor
import org.bson.codecs.kotlin.samples.DataClassWithBsonDiscriminator
import org.bson.codecs.kotlin.samples.DataClassWithBsonExtraElements
import org.bson.codecs.kotlin.samples.DataClassWithBsonId
import org.bson.codecs.kotlin.samples.DataClassWithBsonIgnore
import org.bson.codecs.kotlin.samples.DataClassWithBsonProperty
import org.bson.codecs.kotlin.samples.DataClassWithCollections
import org.bson.codecs.kotlin.samples.DataClassWithDataClassMapKey
import org.bson.codecs.kotlin.samples.DataClassWithDefaults
import org.bson.codecs.kotlin.samples.DataClassWithEmbedded
import org.bson.codecs.kotlin.samples.DataClassWithEnum
import org.bson.codecs.kotlin.samples.DataClassWithEnumMapKey
import org.bson.codecs.kotlin.samples.DataClassWithFailingInit
import org.bson.codecs.kotlin.samples.DataClassWithInvalidBsonRepresentation
import org.bson.codecs.kotlin.samples.DataClassWithMutableList
import org.bson.codecs.kotlin.samples.DataClassWithMutableMap
import org.bson.codecs.kotlin.samples.DataClassWithMutableSet
import org.bson.codecs.kotlin.samples.DataClassWithNestedParameterized
import org.bson.codecs.kotlin.samples.DataClassWithNestedParameterizedDataClass
import org.bson.codecs.kotlin.samples.DataClassWithNulls
import org.bson.codecs.kotlin.samples.DataClassWithObjectIdAndBsonDocument
import org.bson.codecs.kotlin.samples.DataClassWithPair
import org.bson.codecs.kotlin.samples.DataClassWithParameterizedDataClass
import org.bson.codecs.kotlin.samples.DataClassWithSequence
import org.bson.codecs.kotlin.samples.DataClassWithSimpleValues
import org.bson.codecs.kotlin.samples.DataClassWithTriple
import org.bson.codecs.kotlin.samples.Key
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class DataClassCodecTest {
    private val numberLong = "\$numberLong"
    private val emptyDocument = "{}"

    @Test
    fun testDataClassWithSimpleValues() {
        val expected =
            """{"char": "c", "byte": 0, "short": 1, "int": 22, "long": {"$numberLong": "42"}, "float": 4.0,
                | "double": 4.2, "boolean": true, "string": "String"}"""
                .trimMargin()
        val dataClass = DataClassWithSimpleValues('c', 0, 1, 22, 42L, 4.0f, 4.2, true, "String")

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithComplexTypes() {
        val expected =
            """{
            | "listSimple": ["a", "b", "c", "d"],
            | "listList":  [["a", "b"], [], ["c", "d"]],
            | "listMap":  [{"a": 1, "b": 2}, {}, {"c": 3, "d": 4}],
            | "mapSimple": {"a": 1, "b": 2, "c": 3, "d": 4},
            | "mapList": {"a": ["a", "b"], "b": [], "c": ["c", "d"]},
            | "mapMap" : {"a": {"a": 1, "b": 2}, "b": {}, "c": {"c": 3, "d": 4}}
            |}"""
                .trimMargin()

        val dataClass =
            DataClassWithCollections(
                listOf("a", "b", "c", "d"),
                listOf(listOf("a", "b"), emptyList(), listOf("c", "d")),
                listOf(mapOf("a" to 1, "b" to 2), emptyMap(), mapOf("c" to 3, "d" to 4)),
                mapOf("a" to 1, "b" to 2, "c" to 3, "d" to 4),
                mapOf("a" to listOf("a", "b"), "b" to emptyList(), "c" to listOf("c", "d")),
                mapOf("a" to mapOf("a" to 1, "b" to 2), "b" to emptyMap(), "c" to mapOf("c" to 3, "d" to 4)))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithDefaults() {
        val expectedDefault =
            """{
            | "boolean": false,
            | "string": "String",
            | "listSimple": ["a", "b", "c"]
            |}"""
                .trimMargin()

        val defaultDataClass = DataClassWithDefaults()
        assertRoundTrips(expectedDefault, defaultDataClass)
    }

    @Test
    fun testDataClassWithNulls() {
        val dataClass = DataClassWithNulls(null, null, null)
        assertRoundTrips(emptyDocument, dataClass)
    }

    @Test
    fun testDataClassSelfReferential() {
        val expected =
            """{"name": "tree",
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
                    DataClassSelfReferential("RR")))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithEmbedded() {
        val expected = """{"id": "myId", "embedded": {"name": "embedded1"}}"""
        val dataClass = DataClassWithEmbedded("myId", DataClassEmbedded("embedded1"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassListOfDataClasses() {
        val expected = """{"id": "myId", "nested": [{"name": "embedded1"}, {"name": "embedded2"}]}"""
        val dataClass =
            DataClassListOfDataClasses("myId", listOf(DataClassEmbedded("embedded1"), DataClassEmbedded("embedded2")))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassListOfListOfDataClasses() {
        val expected = """{"id": "myId", "nested": [[{"name": "embedded1"}], [{"name": "embedded2"}]]}"""
        val dataClass =
            DataClassListOfListOfDataClasses(
                "myId", listOf(listOf(DataClassEmbedded("embedded1")), listOf(DataClassEmbedded("embedded2"))))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassMapOfDataClasses() {
        val expected = """{"id": "myId", "nested": {"first": {"name": "embedded1"}, "second": {"name": "embedded2"}}}"""
        val dataClass =
            DataClassMapOfDataClasses(
                "myId", mapOf("first" to DataClassEmbedded("embedded1"), "second" to DataClassEmbedded("embedded2")))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassMapOfListOfDataClasses() {
        val expected =
            """{"id": "myId", "nested": {"first": [{"name": "embedded1"}], "second": [{"name": "embedded2"}]}}"""
        val dataClass =
            DataClassMapOfListOfDataClasses(
                "myId",
                mapOf(
                    "first" to listOf(DataClassEmbedded("embedded1")),
                    "second" to listOf(DataClassEmbedded("embedded2"))))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithParameterizedDataClass() {
        val expected =
            """{"id": "myId",
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
            """{"id": "myId",
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
    fun testDataClassWithMutableList() {
        val expected = """{"value": ["A", "B", "C"]}"""
        val dataClass = DataClassWithMutableList(mutableListOf("A", "B", "C"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithMutableSet() {
        val expected = """{"value": ["A", "B", "C"]}"""
        val dataClass = DataClassWithMutableSet(mutableSetOf("A", "B", "C"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithMutableMap() {
        val expected = """{"value": {"a": "A", "b": "B", "c": "C"}}"""
        val dataClass = DataClassWithMutableMap(mutableMapOf("a" to "A", "b" to "B", "c" to "C"))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithEnum() {
        val expected = """{"value": "A"}"""

        val dataClass = DataClassWithEnum(Key.A)
        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithEnumKeyMap() {
        assertThrows<CodecConfigurationException>("Unsupported map key") {
            DataClassCodec.create(DataClassWithEnumMapKey::class, registry())
        }
    }

    @Test
    fun testDataClassWithSequence() {
        assertThrows<CodecConfigurationException>("Unsupported type Sequence") {
            DataClassCodec.create(DataClassWithSequence::class, registry())
        }
    }

    @Test
    fun testDataClassWithBooleanKeyMap() {
        assertThrows<CodecConfigurationException>("Unsupported Map key type") {
            DataClassCodec.create(DataClassWithBooleanMapKey::class, registry())
        }
    }

    @Test
    fun testDataClassWithDataClassKeyMap() {
        assertThrows<CodecConfigurationException>("Unsupported Map key type") {
            DataClassCodec.create(DataClassWithDataClassMapKey::class, registry())
        }
    }

    @Test
    fun testDataClassEmbeddedWithExtraData() {
        val expected =
            """{
            | "extraA": "extraA",
            | "name": "NAME",
            | "extraB": "extraB"
            |}"""
                .trimMargin()

        val dataClass = DataClassEmbedded("NAME")
        assertDecodesTo(BsonDocument.parse(expected), dataClass)
    }

    @Test
    fun testDataClassWithObjectIdAndBsonDocument() {
        val subDocument =
            """{
    | "_id": 1,
    | "arrayEmpty": [],
    | "arraySimple": [{"${'$'}numberInt": "1"}, {"${'$'}numberInt": "2"}, {"${'$'}numberInt": "3"}],
    | "arrayComplex": [{"a": {"${'$'}numberInt": "1"}}, {"a": {"${'$'}numberInt": "2"}}],
    | "arrayMixedTypes": [{"${'$'}numberInt": "1"}, {"${'$'}numberInt": "2"}, true,
    |  [{"${'$'}numberInt": "1"}, {"${'$'}numberInt": "2"}, {"${'$'}numberInt": "3"}],
    |  {"a": {"${'$'}numberInt": "2"}}],
    | "arrayComplexMixedTypes": [{"a": {"${'$'}numberInt": "1"}}, {"a": "a"}],
    | "binary": {"${'$'}binary": {"base64": "S2Fma2Egcm9ja3Mh", "subType": "00"}},
    | "boolean": true,
    | "code": {"${'$'}code": "int i = 0;"},
    | "codeWithScope": {"${'$'}code": "int x = y", "${'$'}scope": {"y": {"${'$'}numberInt": "1"}}},
    | "dateTime": {"${'$'}date": {"${'$'}numberLong": "1577836801000"}},
    | "decimal128": {"${'$'}numberDecimal": "1.0"},
    | "documentEmpty": {},
    | "document": {"a": {"${'$'}numberInt": "1"}},
    | "double": {"${'$'}numberDouble": "62.0"},
    | "int32": {"${'$'}numberInt": "42"},
    | "int64": {"${'$'}numberLong": "52"},
    | "maxKey": {"${'$'}maxKey": 1},
    | "minKey": {"${'$'}minKey": 1},
    | "null": null,
    | "objectId": {"${'$'}oid": "5f3d1bbde0ca4d2829c91e1d"},
    | "regex": {"${'$'}regularExpression": {"pattern": "^test.*regex.*xyz$", "options": "i"}},
    | "string": "the fox ...",
    | "symbol": {"${'$'}symbol": "ruby stuff"},
    | "timestamp": {"${'$'}timestamp": {"t": 305419896, "i": 5}},
    | "undefined": {"${'$'}undefined": true}
    | }"""
                .trimMargin()
        val expected = """{"objectId": {"${'$'}oid": "111111111111111111111111"}, "bsonDocument": $subDocument}"""

        val dataClass =
            DataClassWithObjectIdAndBsonDocument(ObjectId("111111111111111111111111"), BsonDocument.parse(subDocument))
        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassSealed() {
        val dataClassA = DataClassSealedA("string")
        val dataClassB = DataClassSealedB(1)
        val dataClassC = DataClassSealedC("String")

        val expectedDataClassSealedA = """{"a": "string"}"""
        assertRoundTrips(expectedDataClassSealedA, dataClassA)

        val expectedDataClassSealedB = """{"b": 1}"""
        assertRoundTrips(expectedDataClassSealedB, dataClassB)

        val expectedDataClassSealedC = """{"c": "String"}"""
        assertRoundTrips(expectedDataClassSealedC, dataClassC)

        assertThrows<CodecConfigurationException>("No Codec for DataClassSealed") {
            DataClassCodec.create(DataClassListOfSealed::class, registry())
        }
    }

    @Test
    fun testDataFailures() {
        assertThrows<CodecConfigurationException>("Missing data") {
            val codec = DataClassCodec.create(DataClassWithSimpleValues::class, registry())
            codec?.decode(BsonDocumentReader(BsonDocument()), DecoderContext.builder().build())
        }

        assertThrows<CodecConfigurationException>("Invalid types") {
            val data =
                BsonDocument.parse(
                    """{"char": 123, "short": "2", "int": 22, "long": "ok", "float": true, "double": false,
                         | "boolean": "true", "string": 99}"""
                        .trimMargin())
            val codec = DataClassCodec.create(DataClassWithSimpleValues::class, registry())
            codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }

        assertThrows<CodecConfigurationException>("Invalid complex types") {
            val data = BsonDocument.parse("""{"_id": "myId", "embedded": 123}""")
            val codec = DataClassCodec.create(DataClassWithEmbedded::class, registry())
            codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }

        assertThrows<CodecConfigurationException>("Failing init") {
            val data = BsonDocument.parse("""{"id": "myId"}""")
            val codec = DataClassCodec.create(DataClassWithFailingInit::class, registry())
            codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }
    }

    @Test
    fun testSupportedAnnotations() {
        assertRoundTrips("""{"_id": "a"}""", DataClassWithBsonId("a"))
        assertRoundTrips("""{"_id": "a"}""", DataClassWithBsonProperty("a"))
    }

    @Test
    fun testInvalidAnnotations() {
        assertThrows<CodecConfigurationException> {
            DataClassCodec.create(DataClassWithBsonDiscriminator::class, registry())
        }
        assertThrows<CodecConfigurationException> {
            DataClassCodec.create(DataClassWithBsonConstructor::class, registry())
        }
        assertThrows<CodecConfigurationException> { DataClassCodec.create(DataClassWithBsonIgnore::class, registry()) }
        assertThrows<CodecConfigurationException> {
            DataClassCodec.create(DataClassWithBsonExtraElements::class, registry())
        }
        assertThrows<CodecConfigurationException> {
            DataClassCodec.create(DataClassWithInvalidBsonRepresentation::class, registry())
        }
    }

    private fun <T : Any> assertRoundTrips(expected: String, value: T) {
        assertDecodesTo(assertEncodesTo(expected, value), value)
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> assertEncodesTo(json: String, value: T): BsonDocument {
        val expected = BsonDocument.parse(json)
        val codec: DataClassCodec<T> = DataClassCodec.create(value::class, registry()) as DataClassCodec<T>
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
    private fun <T : Any> assertDecodesTo(value: BsonDocument, expected: T) {
        val codec: DataClassCodec<T> = DataClassCodec.create(expected::class, registry()) as DataClassCodec<T>
        val decoded: T = codec.decode(BsonDocumentReader(value), DecoderContext.builder().build())

        assertEquals(expected, decoded)
    }

    private fun registry() = fromProviders(DataClassCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY)
}
