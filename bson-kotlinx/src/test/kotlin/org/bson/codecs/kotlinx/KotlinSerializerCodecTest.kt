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

import kotlin.test.assertEquals
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.MissingFieldException
import kotlinx.serialization.SerializationException
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.plus
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInvalidOperationException
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.kotlinx.samples.DataClassContainsOpen
import org.bson.codecs.kotlinx.samples.DataClassContainsValueClass
import org.bson.codecs.kotlinx.samples.DataClassEmbedded
import org.bson.codecs.kotlinx.samples.DataClassKey
import org.bson.codecs.kotlinx.samples.DataClassListOfDataClasses
import org.bson.codecs.kotlinx.samples.DataClassListOfListOfDataClasses
import org.bson.codecs.kotlinx.samples.DataClassListOfSealed
import org.bson.codecs.kotlinx.samples.DataClassMapOfDataClasses
import org.bson.codecs.kotlinx.samples.DataClassMapOfListOfDataClasses
import org.bson.codecs.kotlinx.samples.DataClassNestedParameterizedTypes
import org.bson.codecs.kotlinx.samples.DataClassOpen
import org.bson.codecs.kotlinx.samples.DataClassOpenA
import org.bson.codecs.kotlinx.samples.DataClassOpenB
import org.bson.codecs.kotlinx.samples.DataClassParameterized
import org.bson.codecs.kotlinx.samples.DataClassSealed
import org.bson.codecs.kotlinx.samples.DataClassSealedA
import org.bson.codecs.kotlinx.samples.DataClassSealedB
import org.bson.codecs.kotlinx.samples.DataClassSealedC
import org.bson.codecs.kotlinx.samples.DataClassSelfReferential
import org.bson.codecs.kotlinx.samples.DataClassWithAnnotations
import org.bson.codecs.kotlinx.samples.DataClassWithBooleanMapKey
import org.bson.codecs.kotlinx.samples.DataClassWithBsonConstructor
import org.bson.codecs.kotlinx.samples.DataClassWithBsonDiscriminator
import org.bson.codecs.kotlinx.samples.DataClassWithBsonExtraElements
import org.bson.codecs.kotlinx.samples.DataClassWithBsonId
import org.bson.codecs.kotlinx.samples.DataClassWithBsonIgnore
import org.bson.codecs.kotlinx.samples.DataClassWithBsonProperty
import org.bson.codecs.kotlinx.samples.DataClassWithBsonRepresentation
import org.bson.codecs.kotlinx.samples.DataClassWithCollections
import org.bson.codecs.kotlinx.samples.DataClassWithDataClassMapKey
import org.bson.codecs.kotlinx.samples.DataClassWithDefaults
import org.bson.codecs.kotlinx.samples.DataClassWithEmbedded
import org.bson.codecs.kotlinx.samples.DataClassWithEncodeDefault
import org.bson.codecs.kotlinx.samples.DataClassWithEnum
import org.bson.codecs.kotlinx.samples.DataClassWithEnumMapKey
import org.bson.codecs.kotlinx.samples.DataClassWithFailingInit
import org.bson.codecs.kotlinx.samples.DataClassWithMutableList
import org.bson.codecs.kotlinx.samples.DataClassWithMutableMap
import org.bson.codecs.kotlinx.samples.DataClassWithMutableSet
import org.bson.codecs.kotlinx.samples.DataClassWithNestedParameterized
import org.bson.codecs.kotlinx.samples.DataClassWithNestedParameterizedDataClass
import org.bson.codecs.kotlinx.samples.DataClassWithNulls
import org.bson.codecs.kotlinx.samples.DataClassWithObjectIdAndBsonDocument
import org.bson.codecs.kotlinx.samples.DataClassWithPair
import org.bson.codecs.kotlinx.samples.DataClassWithParameterizedDataClass
import org.bson.codecs.kotlinx.samples.DataClassWithRequired
import org.bson.codecs.kotlinx.samples.DataClassWithSequence
import org.bson.codecs.kotlinx.samples.DataClassWithSimpleValues
import org.bson.codecs.kotlinx.samples.DataClassWithTriple
import org.bson.codecs.kotlinx.samples.Key
import org.bson.codecs.kotlinx.samples.ValueClass
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@OptIn(ExperimentalSerializationApi::class)
class KotlinSerializerCodecTest {
    private val numberLong = "\$numberLong"
    private val emptyDocument = "{}"
    private val altConfiguration =
        BsonConfiguration(encodeDefaults = false, classDiscriminator = "_t", explicitNulls = true)

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
        assertRoundTrips(emptyDocument, defaultDataClass, altConfiguration)

        val expectedSomeOverrides = """{"boolean": true, "listSimple": ["a"]}"""
        val someOverridesDataClass = DataClassWithDefaults(boolean = true, listSimple = listOf("a"))
        assertRoundTrips(expectedSomeOverrides, someOverridesDataClass, altConfiguration)
    }

    @Test
    fun testDataClassWithNulls() {
        val expectedNulls =
            """{
            | "boolean": null,
            | "string": null,
            | "listSimple": null
            |}"""
                .trimMargin()

        val dataClass = DataClassWithNulls(null, null, null)
        assertRoundTrips(emptyDocument, dataClass)
        assertRoundTrips(expectedNulls, dataClass, altConfiguration)
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
    fun testDataClassWithAnnotations() {
        val expected = """{"_id": "id", "nom": "name", "string": "string"}"""
        val dataClass = DataClassWithAnnotations("id", "name", "string")

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithEncodeDefault() {
        val expectedDefault =
            """{
            | "boolean": false,
            | "listSimple": ["a", "b", "c"]
            |}"""
                .trimMargin()

        val defaultDataClass = DataClassWithEncodeDefault()
        assertRoundTrips(expectedDefault, defaultDataClass)
        assertRoundTrips("""{"listSimple": ["a", "b", "c"]}""", defaultDataClass, altConfiguration)

        val expectedSomeOverrides = """{"string": "STRING", "listSimple": ["a"]}"""
        val someOverridesDataClass = DataClassWithEncodeDefault(string = "STRING", listSimple = listOf("a"))
        assertRoundTrips(expectedSomeOverrides, someOverridesDataClass, altConfiguration)
    }

    @Test
    fun testDataClassWithRequired() {
        val expectedDefault =
            """{
            | "boolean": false,
            | "string": "String",
            | "listSimple": ["a", "b", "c"]
            |}"""
                .trimMargin()

        val defaultDataClass = DataClassWithRequired()
        assertRoundTrips(expectedDefault, defaultDataClass)

        assertThrows<MissingFieldException> { deserialize<DataClassWithRequired>(BsonDocument()) }
    }

    @Test
    fun testDataClassWithEnum() {
        val expected = """{"value": "A"}"""

        val dataClass = DataClassWithEnum(Key.A)
        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithEnumKeyMap() {
        val expected = """{"map": {"A": true, "B": false}}"""

        val dataClass = DataClassWithEnumMapKey(mapOf(Key.A to true, Key.B to false))
        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithSequence() {
        val dataClass = DataClassWithSequence(listOf("A", "B", "C").asSequence())
        assertThrows<SerializationException> { serialize(dataClass) }
    }

    @Test
    fun testDataClassWithBooleanKeyMap() {
        val dataClass = DataClassWithBooleanMapKey(mapOf(true to true, false to false))
        assertThrows<SerializationException> { serialize(dataClass) }
        assertThrows<SerializationException> {
            deserialize<DataClassWithBooleanMapKey>(BsonDocument.parse("""{"map": {"true": true}}"""))
        }
    }

    @Test
    fun testDataClassWithDataClassKeyMap() {
        val dataClass = DataClassWithDataClassMapKey(mapOf(DataClassKey("A") to true, DataClassKey("A") to false))
        assertThrows<SerializationException> { serialize(dataClass) }
        assertThrows<SerializationException> {
            deserialize<DataClassWithDataClassMapKey>(BsonDocument.parse("""{"map": {"A": true}}"""))
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
        val expectedA = """{"a": "string"}"""
        val dataClassA = DataClassSealedA("string")
        assertRoundTrips(expectedA, dataClassA)

        val expectedB = """{"b": 1}"""
        val dataClassB = DataClassSealedB(1)
        assertRoundTrips(expectedB, dataClassB)

        val expectedC = """{"c": "String"}"""
        val dataClassC = DataClassSealedC("String")
        assertRoundTrips(expectedC, dataClassC)

        val expectedDataClassSealedA = """{"_t": "org.bson.codecs.kotlinx.samples.DataClassSealedA", "a": "string"}"""
        val dataClassSealedA = DataClassSealedA("string") as DataClassSealed
        assertRoundTrips(expectedDataClassSealedA, dataClassSealedA)

        val expectedDataClassSealedB = """{"_t": "org.bson.codecs.kotlinx.samples.DataClassSealedB", "b": 1}"""
        val dataClassSealedB = DataClassSealedB(1) as DataClassSealed
        assertRoundTrips(expectedDataClassSealedB, dataClassSealedB)

        val expectedDataClassSealedC = """{"_t": "C", "c": "String"}"""
        val dataClassSealedC = DataClassSealedC("String") as DataClassSealed
        assertRoundTrips(expectedDataClassSealedC, dataClassSealedC)

        val dataClassListOfSealed = DataClassListOfSealed(listOf(dataClassA, dataClassB, dataClassC))
        val expectedListOfSealed =
            """{"items": [$expectedDataClassSealedA, $expectedDataClassSealedB, $expectedDataClassSealedC]}"""
        assertRoundTrips(expectedListOfSealed, dataClassListOfSealed)

        val expectedListOfSealedDiscriminator = expectedListOfSealed.replace("_t", "#class")
        assertRoundTrips(
            expectedListOfSealedDiscriminator, dataClassListOfSealed, BsonConfiguration(classDiscriminator = "#class"))
    }

    @Test
    fun testDataClassOpen() {
        val expectedA = """{"a": "string"}"""
        val dataClassA = DataClassOpenA("string")
        assertRoundTrips(expectedA, dataClassA)

        val expectedB = """{"b": 1}"""
        val dataClassB = DataClassOpenB(1)
        assertRoundTrips(expectedB, dataClassB)

        val serializersModule =
            SerializersModule {
                this.polymorphic(DataClassOpen::class) {
                    this.subclass(DataClassOpenA::class)
                    this.subclass(DataClassOpenB::class)
                }
            } + defaultSerializersModule

        val dataClassContainsOpenA = DataClassContainsOpen(dataClassA)
        val expectedOpenA = """{"open": {"_t": "org.bson.codecs.kotlinx.samples.DataClassOpenA", "a": "string"}}"""
        assertRoundTrips(expectedOpenA, dataClassContainsOpenA, serializersModule = serializersModule)

        val dataClassContainsOpenB = DataClassContainsOpen(dataClassB)
        val expectedOpenB = """{"open": {"#class": "org.bson.codecs.kotlinx.samples.DataClassOpenB", "b": 1}}"""
        assertRoundTrips(
            expectedOpenB,
            dataClassContainsOpenB,
            configuration = BsonConfiguration(classDiscriminator = "#class"),
            serializersModule = serializersModule)
    }

    @Test
    fun testValueClasses() {
        val expected = """{"value": "valueString"}"""
        val valueClass = ValueClass("valueString")
        val dataClass =  DataClassContainsValueClass(valueClass)

        assertThrows<BsonInvalidOperationException>() {
            serialize(valueClass)
        }
        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataFailures() {
        assertThrows<MissingFieldException>("Missing data") {
            val codec = KotlinSerializerCodec.create(DataClassWithSimpleValues::class)
            codec?.decode(BsonDocumentReader(BsonDocument()), DecoderContext.builder().build())
        }

        assertThrows<BsonInvalidOperationException>("Invalid types") {
            val data =
                BsonDocument.parse(
                    """{"char": 123, "short": "2", "int": 22, "long": "ok", "float": true, "double": false,
            | "boolean": "true", "string": 99}"""
                        .trimMargin())
            val codec = KotlinSerializerCodec.create<DataClassWithSimpleValues>()
            codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }

        assertThrows<MissingFieldException>("Invalid complex types") {
            val data = BsonDocument.parse("""{"_id": "myId", "embedded": 123}""")
            val codec = KotlinSerializerCodec.create<DataClassWithEmbedded>()
            codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }

        assertThrows<IllegalArgumentException>("Failing init") {
            val data = BsonDocument.parse("""{"id": "myId"}""")
            val codec = KotlinSerializerCodec.create<DataClassWithFailingInit>()
            codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }
    }

    @Test
    fun testInvalidAnnotations() {
        assertThrows<CodecConfigurationException> { KotlinSerializerCodec.create(DataClassWithBsonId::class) }
        assertThrows<CodecConfigurationException> { KotlinSerializerCodec.create(DataClassWithBsonProperty::class) }
        assertThrows<CodecConfigurationException> {
            KotlinSerializerCodec.create(DataClassWithBsonDiscriminator::class)
        }
        assertThrows<CodecConfigurationException> { KotlinSerializerCodec.create(DataClassWithBsonConstructor::class) }
        assertThrows<CodecConfigurationException> { KotlinSerializerCodec.create(DataClassWithBsonIgnore::class) }
        assertThrows<CodecConfigurationException> {
            KotlinSerializerCodec.create(DataClassWithBsonExtraElements::class)
        }
        assertThrows<CodecConfigurationException> {
            KotlinSerializerCodec.create(DataClassWithBsonRepresentation::class)
        }
    }

    private inline fun <reified T : Any> assertRoundTrips(
        expected: String,
        value: T,
        configuration: BsonConfiguration = BsonConfiguration(),
        serializersModule: SerializersModule = defaultSerializersModule
    ) {
        assertDecodesTo(
            assertEncodesTo(expected, value, serializersModule, configuration), value, serializersModule, configuration)
    }

    private inline fun <reified T : Any> assertEncodesTo(
        json: String,
        value: T,
        serializersModule: SerializersModule = defaultSerializersModule,
        configuration: BsonConfiguration = BsonConfiguration()
    ): BsonDocument {
        val expected = BsonDocument.parse(json)
        val actual = serialize(value, serializersModule, configuration)
        assertEquals(expected, actual)
        return actual
    }

    private inline fun <reified T : Any> serialize(
        value: T,
        serializersModule: SerializersModule = defaultSerializersModule,
        configuration: BsonConfiguration = BsonConfiguration()
    ): BsonDocument {
        val document = BsonDocument()
        val writer = BsonDocumentWriter(document)
        val codec = KotlinSerializerCodec.create(T::class, serializersModule, configuration)!!
        codec.encode(writer, value, EncoderContext.builder().build())
        writer.flush()
        return document
    }

    private inline fun <reified T : Any> assertDecodesTo(
        value: BsonDocument,
        expected: T,
        serializersModule: SerializersModule = defaultSerializersModule,
        configuration: BsonConfiguration = BsonConfiguration()
    ) {
        assertEquals(expected, deserialize(value, serializersModule, configuration))
    }
    private inline fun <reified T : Any> deserialize(
        value: BsonDocument,
        serializersModule: SerializersModule = defaultSerializersModule,
        configuration: BsonConfiguration = BsonConfiguration()
    ): T {
        val codec = KotlinSerializerCodec.create(T::class, serializersModule, configuration)!!
        return codec.decode(BsonDocumentReader(value), DecoderContext.builder().build())
    }
}
