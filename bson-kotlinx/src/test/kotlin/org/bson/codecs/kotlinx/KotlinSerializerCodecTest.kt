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

import java.math.BigDecimal
import java.util.Base64
import java.util.stream.Stream
import kotlin.test.assertEquals
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.MissingFieldException
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.plus
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonInvalidOperationException
import org.bson.BsonMaxKey
import org.bson.BsonMinKey
import org.bson.BsonString
import org.bson.BsonUndefined
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.kotlinx.samples.*
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

@OptIn(ExperimentalSerializationApi::class)
@Suppress("LargeClass")
class KotlinSerializerCodecTest {
    private val oid = "\$oid"
    private val numberLong = "\$numberLong"
    private val numberDecimal = "\$numberDecimal"
    private val emptyDocument = "{}"
    private val altConfiguration =
        BsonConfiguration(encodeDefaults = false, classDiscriminator = "_t", explicitNulls = true)

    private val allBsonTypesJson =
        """{
    | "id": {"$oid": "111111111111111111111111"},
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
    | "codeWithScope": {"${'$'}code": "int x = y", "${'$'}scope": {"y": 1}},
    | "dateTime": {"${'$'}date": {"${'$'}numberLong": "1577836801000"}},
    | "decimal128": {"${'$'}numberDecimal": "1.0"},
    | "documentEmpty": {},
    | "document": {"a": {"${'$'}numberInt": "1"}},
    | "double": {"${'$'}numberDouble": "62.0"},
    | "int32": {"${'$'}numberInt": "42"},
    | "int64": {"${'$'}numberLong": "52"},
    | "maxKey": {"${'$'}maxKey": 1},
    | "minKey": {"${'$'}minKey": 1},
    | "objectId": {"${'$'}oid": "211111111111111111111112"},
    | "regex": {"${'$'}regularExpression": {"pattern": "^test.*regex.*xyz$", "options": "i"}},
    | "string": "the fox ...",
    | "symbol": {"${'$'}symbol": "ruby stuff"},
    | "timestamp": {"${'$'}timestamp": {"t": 305419896, "i": 5}},
    | "undefined": {"${'$'}undefined": true}
    | }"""
            .trimMargin()

    private val allBsonTypesDocument = BsonDocument.parse(allBsonTypesJson)
    private val jsonAllSupportedTypesDocument: BsonDocument by
        lazy<BsonDocument> {
            val doc = BsonDocument.parse(allBsonTypesJson)
            listOf("minKey", "maxKey", "code", "codeWithScope", "regex", "symbol", "undefined").forEach {
                doc.remove(it)
            }
            doc
        }

    companion object {
        @JvmStatic
        fun testTypesCastingDataClassWithSimpleValues(): Stream<BsonDocument> {
            return Stream.of(
                BsonDocument()
                    .append("char", BsonString("c"))
                    .append("byte", BsonInt32(1))
                    .append("short", BsonInt32(2))
                    .append("int", BsonInt32(10))
                    .append("long", BsonInt32(10))
                    .append("float", BsonInt32(2))
                    .append("double", BsonInt32(3))
                    .append("boolean", BsonBoolean.TRUE)
                    .append("string", BsonString("String")),
                BsonDocument()
                    .append("char", BsonString("c"))
                    .append("byte", BsonDouble(1.0))
                    .append("short", BsonDouble(2.0))
                    .append("int", BsonDouble(9.9999999999999992))
                    .append("long", BsonDouble(9.9999999999999992))
                    .append("float", BsonDouble(2.0))
                    .append("double", BsonDouble(3.0))
                    .append("boolean", BsonBoolean.TRUE)
                    .append("string", BsonString("String")),
                BsonDocument()
                    .append("char", BsonString("c"))
                    .append("byte", BsonDouble(1.0))
                    .append("short", BsonDouble(2.0))
                    .append("int", BsonDouble(10.0))
                    .append("long", BsonDouble(10.0))
                    .append("float", BsonDouble(2.0))
                    .append("double", BsonDouble(3.0))
                    .append("boolean", BsonBoolean.TRUE)
                    .append("string", BsonString("String")),
                BsonDocument()
                    .append("char", BsonString("c"))
                    .append("byte", BsonInt64(1))
                    .append("short", BsonInt64(2))
                    .append("int", BsonInt64(10))
                    .append("long", BsonInt64(10))
                    .append("float", BsonInt64(2))
                    .append("double", BsonInt64(3))
                    .append("boolean", BsonBoolean.TRUE)
                    .append("string", BsonString("String")))
        }
    }

    @ParameterizedTest
    @MethodSource("testTypesCastingDataClassWithSimpleValues")
    fun testTypesCastingDataClassWithSimpleValues(data: BsonDocument) {
        val expectedDataClass = DataClassWithSimpleValues('c', 1, 2, 10, 10L, 2.0f, 3.0, true, "String")

        assertDecodesTo(data, expectedDataClass)
    }

    @Test
    fun testDataClassWithDateValuesContextualSerialization() {
        val expected =
            "{\n" +
                "    \"instant\": {\"\$date\": \"2001-09-09T01:46:40Z\"}, \n" +
                "    \"localTime\": {\"\$date\": \"1970-01-01T00:00:10Z\"}, \n" +
                "    \"localDateTime\": {\"\$date\": \"2021-01-01T00:00:04Z\"}, \n" +
                "    \"localDate\": {\"\$date\": \"1970-10-28T00:00:00Z\"}\n" +
                "}".trimMargin()

        val expectedDataClass =
            DataClassWithContextualDateValues(
                Instant.fromEpochMilliseconds(10_000_000_000_00),
                LocalTime.fromMillisecondOfDay(10_000),
                LocalDateTime.parse("2021-01-01T00:00:04"),
                LocalDate.fromEpochDays(300))

        assertRoundTrips(expected, expectedDataClass)
    }

    @Test
    fun testDataClassWithDateValuesStandard() {
        val expected =
            "{\n" +
                "    \"instant\": \"1970-01-01T00:00:01Z\", \n" +
                "    \"localTime\": \"00:00:01\", \n" +
                "    \"localDateTime\": \"2021-01-01T00:00:04\", \n" +
                "    \"localDate\":  \"1970-01-02\"\n" +
                "}".trimMargin()

        val expectedDataClass =
            DataClassWithDateValues(
                Instant.fromEpochMilliseconds(1000),
                LocalTime.fromMillisecondOfDay(1000),
                LocalDateTime.parse("2021-01-01T00:00:04"),
                LocalDate.fromEpochDays(1))

        assertRoundTrips(expected, expectedDataClass)
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
    fun testDataClassWithListThatLastItemDefaultsToNull() {
        val expectedWithOutNulls =
            """{
            | "elements": [{"required": "required"}, {"required": "required"}],
            |}"""
                .trimMargin()

        val dataClass =
            DataClassWithListThatLastItemDefaultsToNull(
                listOf(DataClassLastItemDefaultsToNull("required"), DataClassLastItemDefaultsToNull("required")))
        assertRoundTrips(expectedWithOutNulls, dataClass)

        val expectedWithNulls =
            """{
            | "elements": [{"required": "required", "optional": null}, {"required": "required", "optional": null}],
            |}"""
                .trimMargin()
        assertRoundTrips(expectedWithNulls, dataClass, BsonConfiguration(explicitNulls = true))
    }

    @Test
    fun testDataClassWithNullableGenericsNotNull() {
        val expected =
            """{
            | "box": {"boxed": "String"}
            |}"""
                .trimMargin()

        val dataClass = DataClassWithNullableGeneric(Box("String"))
        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithNullableGenericsNull() {
        val expectedDefault = """{"box": {}}"""
        val dataClass = DataClassWithNullableGeneric(Box(null))
        assertRoundTrips(expectedDefault, dataClass)
        val expectedNull = """{"box": {"boxed": null}}"""
        assertRoundTrips(expectedNull, dataClass, altConfiguration)
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
            |  "other": "myOtherString", "optionalOther": "myOptionalOtherString"
            | }
            |}"""
                .trimMargin()
        val dataClass =
            DataClassWithNestedParameterizedDataClass(
                "myId",
                DataClassWithNestedParameterized(
                    DataClassParameterized(4.2, "myString", listOf(DataClassEmbedded("embedded1"))),
                    "myOtherString",
                    "myOptionalOtherString"))

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
    fun testDataClassBsonValues() {

        val dataClass =
            DataClassBsonValues(
                allBsonTypesDocument["id"]!!.asObjectId().value,
                allBsonTypesDocument["arrayEmpty"]!!.asArray(),
                allBsonTypesDocument["arraySimple"]!!.asArray(),
                allBsonTypesDocument["arrayComplex"]!!.asArray(),
                allBsonTypesDocument["arrayMixedTypes"]!!.asArray(),
                allBsonTypesDocument["arrayComplexMixedTypes"]!!.asArray(),
                allBsonTypesDocument["binary"]!!.asBinary(),
                allBsonTypesDocument["boolean"]!!.asBoolean(),
                allBsonTypesDocument["code"]!!.asJavaScript(),
                allBsonTypesDocument["codeWithScope"]!!.asJavaScriptWithScope(),
                allBsonTypesDocument["dateTime"]!!.asDateTime(),
                allBsonTypesDocument["decimal128"]!!.asDecimal128(),
                allBsonTypesDocument["documentEmpty"]!!.asDocument(),
                allBsonTypesDocument["document"]!!.asDocument(),
                allBsonTypesDocument["double"]!!.asDouble(),
                allBsonTypesDocument["int32"]!!.asInt32(),
                allBsonTypesDocument["int64"]!!.asInt64(),
                allBsonTypesDocument["maxKey"]!! as BsonMaxKey,
                allBsonTypesDocument["minKey"]!! as BsonMinKey,
                allBsonTypesDocument["objectId"]!!.asObjectId(),
                allBsonTypesDocument["regex"]!!.asRegularExpression(),
                allBsonTypesDocument["string"]!!.asString(),
                allBsonTypesDocument["symbol"]!!.asSymbol(),
                allBsonTypesDocument["timestamp"]!!.asTimestamp(),
                allBsonTypesDocument["undefined"]!! as BsonUndefined)

        assertRoundTrips(allBsonTypesJson, dataClass)
    }

    @Test
    fun testDataClassOptionalBsonValues() {
        val dataClass =
            DataClassOptionalBsonValues(
                allBsonTypesDocument["id"]!!.asObjectId().value,
                allBsonTypesDocument["arrayEmpty"]!!.asArray(),
                allBsonTypesDocument["arraySimple"]!!.asArray(),
                allBsonTypesDocument["arrayComplex"]!!.asArray(),
                allBsonTypesDocument["arrayMixedTypes"]!!.asArray(),
                allBsonTypesDocument["arrayComplexMixedTypes"]!!.asArray(),
                allBsonTypesDocument["binary"]!!.asBinary(),
                allBsonTypesDocument["boolean"]!!.asBoolean(),
                allBsonTypesDocument["code"]!!.asJavaScript(),
                allBsonTypesDocument["codeWithScope"]!!.asJavaScriptWithScope(),
                allBsonTypesDocument["dateTime"]!!.asDateTime(),
                allBsonTypesDocument["decimal128"]!!.asDecimal128(),
                allBsonTypesDocument["documentEmpty"]!!.asDocument(),
                allBsonTypesDocument["document"]!!.asDocument(),
                allBsonTypesDocument["double"]!!.asDouble(),
                allBsonTypesDocument["int32"]!!.asInt32(),
                allBsonTypesDocument["int64"]!!.asInt64(),
                allBsonTypesDocument["maxKey"]!! as BsonMaxKey,
                allBsonTypesDocument["minKey"]!! as BsonMinKey,
                allBsonTypesDocument["objectId"]!!.asObjectId(),
                allBsonTypesDocument["regex"]!!.asRegularExpression(),
                allBsonTypesDocument["string"]!!.asString(),
                allBsonTypesDocument["symbol"]!!.asSymbol(),
                allBsonTypesDocument["timestamp"]!!.asTimestamp(),
                allBsonTypesDocument["undefined"]!! as BsonUndefined)

        assertRoundTrips(allBsonTypesJson, dataClass)

        val emptyDataClass =
            DataClassOptionalBsonValues(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null)

        assertRoundTrips("{}", emptyDataClass)
        assertRoundTrips(
            """{ "id": null,  "arrayEmpty": null, "arraySimple": null, "arrayComplex": null, "arrayMixedTypes": null,
                | "arrayComplexMixedTypes": null, "binary": null, "boolean": null, "code": null, "codeWithScope": null,
                | "dateTime": null, "decimal128": null, "documentEmpty": null, "document": null, "double": null,
                | "int32": null, "int64": null, "maxKey": null, "minKey": null, "objectId": null, "regex": null,
                | "string": null, "symbol": null, "timestamp": null, "undefined": null }"""
                .trimMargin(),
            emptyDataClass,
            BsonConfiguration(explicitNulls = true))
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
        val dataClass = DataClassContainsValueClass(valueClass)

        assertThrows<BsonInvalidOperationException>() { serialize(valueClass) }
        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithJsonElement() {
        val expected =
            """{"value": {
            |"char": "c",
            |"byte": 0,
            |"short": 1,
            |"int":  22,
            |"long": {"$numberLong": "3000000000"},
            |"decimal": {"$numberDecimal": "10000000000000000000"}
            |"decimal2": {"$numberDecimal": "3.1230E+700"}
            |"float": 4.0,
            |"double": 4.2,
            |"boolean": true,
            |"string": "String"
            |}}"""
                .trimMargin()

        val dataClass =
            DataClassWithJsonElement(
                buildJsonObject {
                    put("char", "c")
                    put("byte", 0)
                    put("short", 1)
                    put("int", 22)
                    put("long", 3_000_000_000)
                    put("decimal", BigDecimal("10000000000000000000"))
                    put("decimal2", BigDecimal("3.1230E+700"))
                    put("float", 4.0)
                    put("double", 4.2)
                    put("boolean", true)
                    put("string", "String")
                })

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithJsonElements() {
        val expected =
            """{
                | "jsonElement": {"string": "String"},
                | "jsonArray": [1, 2],
                | "jsonElements": [{"string": "String"}, {"int": 42}],
                | "jsonNestedMap": {"nestedString": {"string": "String"},
                |    "nestedLong": {"long": {"$numberLong": "3000000000"}}}
                |}"""
                .trimMargin()

        val dataClass =
            DataClassWithJsonElements(
                buildJsonObject { put("string", "String") },
                buildJsonArray {
                    add(JsonPrimitive(1))
                    add(JsonPrimitive(2))
                },
                listOf(buildJsonObject { put("string", "String") }, buildJsonObject { put("int", 42) }),
                mapOf(
                    Pair("nestedString", buildJsonObject { put("string", "String") }),
                    Pair("nestedLong", buildJsonObject { put("long", 3000000000L) })))

        assertRoundTrips(expected, dataClass)
    }

    @Test
    fun testDataClassWithJsonElementsNullable() {
        val expected =
            """{
                | "jsonElement": {"null": null},
                | "jsonArray": [1, 2, null],
                | "jsonElements": [{"null": null}],
                | "jsonNestedMap": {"nestedNull": null}
                |}"""
                .trimMargin()

        val dataClass =
            DataClassWithJsonElementsNullable(
                buildJsonObject { put("null", null) },
                buildJsonArray {
                    add(JsonPrimitive(1))
                    add(JsonPrimitive(2))
                    add(JsonPrimitive(null))
                },
                listOf(buildJsonObject { put("null", null) }),
                mapOf(Pair("nestedNull", null)))

        assertRoundTrips(expected, dataClass, altConfiguration)

        val expectedNoNulls =
            """{
                | "jsonElement": {},
                | "jsonArray": [1, 2],
                | "jsonElements": [{}],
                | "jsonNestedMap": {}
                |}"""
                .trimMargin()

        val dataClassNoNulls =
            DataClassWithJsonElementsNullable(
                buildJsonObject {},
                buildJsonArray {
                    add(JsonPrimitive(1))
                    add(JsonPrimitive(2))
                },
                listOf(buildJsonObject {}),
                mapOf())
        assertEncodesTo(expectedNoNulls, dataClass)
        assertDecodesTo(expectedNoNulls, dataClassNoNulls)
    }

    @Test
    fun testDataClassWithJsonElementNullSupport() {
        val expected =
            """{"jsonElement": {"null": null},
                | "jsonArray": [1, 2, null],
                | "jsonElements": [{"null": null}],
                | "jsonNestedMap": {"nestedNull": null}
                | }
                | """
                .trimMargin()

        val dataClass =
            DataClassWithJsonElements(
                buildJsonObject { put("null", null) },
                buildJsonArray {
                    add(JsonPrimitive(1))
                    add(JsonPrimitive(2))
                    add(JsonPrimitive(null))
                },
                listOf(buildJsonObject { put("null", null) }),
                mapOf(Pair("nestedNull", JsonPrimitive(null))))

        assertRoundTrips(expected, dataClass, altConfiguration)

        val expectedNoNulls =
            """{"jsonElement": {},
                | "jsonArray": [1, 2],
                | "jsonElements": [{}],
                | "jsonNestedMap": {}
                | }
                | """
                .trimMargin()

        val dataClassNoNulls =
            DataClassWithJsonElements(
                buildJsonObject {},
                buildJsonArray {
                    add(JsonPrimitive(1))
                    add(JsonPrimitive(2))
                },
                listOf(buildJsonObject {}),
                mapOf())
        assertEncodesTo(expectedNoNulls, dataClass)
        assertDecodesTo(expectedNoNulls, dataClassNoNulls)
    }

    @Test
    @Suppress("LongMethod")
    fun testDataClassWithJsonElementBsonSupport() {
        val dataClassWithAllSupportedJsonTypes =
            DataClassWithJsonElement(
                buildJsonObject {
                    put("id", "111111111111111111111111")
                    put("arrayEmpty", buildJsonArray {})
                    put(
                        "arraySimple",
                        buildJsonArray {
                            add(JsonPrimitive(1))
                            add(JsonPrimitive(2))
                            add(JsonPrimitive(3))
                        })
                    put(
                        "arrayComplex",
                        buildJsonArray {
                            add(buildJsonObject { put("a", JsonPrimitive(1)) })
                            add(buildJsonObject { put("a", JsonPrimitive(2)) })
                        })
                    put(
                        "arrayMixedTypes",
                        buildJsonArray {
                            add(JsonPrimitive(1))
                            add(JsonPrimitive(2))
                            add(JsonPrimitive(true))
                            add(
                                buildJsonArray {
                                    add(JsonPrimitive(1))
                                    add(JsonPrimitive(2))
                                    add(JsonPrimitive(3))
                                })
                            add(buildJsonObject { put("a", JsonPrimitive(2)) })
                        })
                    put(
                        "arrayComplexMixedTypes",
                        buildJsonArray {
                            add(buildJsonObject { put("a", JsonPrimitive(1)) })
                            add(buildJsonObject { put("a", JsonPrimitive("a")) })
                        })
                    put("binary", JsonPrimitive("S2Fma2Egcm9ja3Mh"))
                    put("boolean", JsonPrimitive(true))
                    put("dateTime", JsonPrimitive(1577836801000))
                    put("decimal128", JsonPrimitive(1.0))
                    put("documentEmpty", buildJsonObject {})
                    put("document", buildJsonObject { put("a", JsonPrimitive(1)) })
                    put("double", JsonPrimitive(62.0))
                    put("int32", JsonPrimitive(42))
                    put("int64", JsonPrimitive(52))
                    put("objectId", JsonPrimitive("211111111111111111111112"))
                    put("string", JsonPrimitive("the fox ..."))
                    put("timestamp", JsonPrimitive(1311768464867721221))
                })

        val jsonWriterSettings =
            JsonWriterSettings.builder()
                .outputMode(JsonMode.RELAXED)
                .objectIdConverter { oid, writer -> writer.writeString(oid.toHexString()) }
                .dateTimeConverter { d, writer -> writer.writeNumber(d.toString()) }
                .timestampConverter { ts, writer -> writer.writeNumber(ts.value.toString()) }
                .binaryConverter { b, writer -> writer.writeString(Base64.getEncoder().encodeToString(b.data)) }
                .decimal128Converter { d, writer -> writer.writeNumber(d.toDouble().toString()) }
                .build()
        val dataClassWithAllSupportedJsonTypesSimpleJson = jsonAllSupportedTypesDocument.toJson(jsonWriterSettings)

        assertEncodesTo(
            """{"value": $dataClassWithAllSupportedJsonTypesSimpleJson }""", dataClassWithAllSupportedJsonTypes)
        assertDecodesTo("""{"value": $jsonAllSupportedTypesDocument}""", dataClassWithAllSupportedJsonTypes)
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

        assertThrows<IllegalArgumentException>("Failing init") {
            val data = BsonDocument.parse("""{"id": "myId"}""")
            val codec = KotlinSerializerCodec.create<DataClassWithFailingInit>()
            codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
        }

        var exception =
            assertThrows<SerializationException>("Invalid complex types - document") {
                val data = BsonDocument.parse("""{"_id": "myId", "embedded": 123}""")
                val codec = KotlinSerializerCodec.create<DataClassWithEmbedded>()
                codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
            }
        assertEquals(
            "Invalid data for `org.bson.codecs.kotlinx.samples.DataClassEmbedded` " +
                "expected a bson document found: INT32",
            exception.message)

        exception =
            assertThrows<SerializationException>("Invalid complex types - list") {
                val data = BsonDocument.parse("""{"_id": "myId", "nested": 123}""")
                val codec = KotlinSerializerCodec.create<DataClassListOfDataClasses>()
                codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
            }
        assertEquals("Invalid data for `LIST` expected a bson array found: INT32", exception.message)

        exception =
            assertThrows<SerializationException>("Invalid complex types - map") {
                val data = BsonDocument.parse("""{"_id": "myId", "nested": 123}""")
                val codec = KotlinSerializerCodec.create<DataClassMapOfDataClasses>()
                codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
            }
        assertEquals("Invalid data for `MAP` expected a bson document found: INT32", exception.message)

        exception =
            assertThrows<SerializationException>("Missing discriminator") {
                val data = BsonDocument.parse("""{"_id": {"$oid": "111111111111111111111111"}, "name": "string"}""")
                val codec = KotlinSerializerCodec.create<SealedInterface>()
                codec?.decode(BsonDocumentReader(data), DecoderContext.builder().build())
            }
        assertEquals(
            "Missing required discriminator field `_t` for polymorphic class: " +
                "`org.bson.codecs.kotlinx.samples.SealedInterface`.",
            exception.message)
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

    @Test
    fun testSnakeCaseNamingStrategy() {
        val expected =
            """{"two_words": "", "my_property": "", "camel_case_underscores": "", "url_mapping": "",
            | "my_http_auth": "", "my_http2_api_key": "", "my_http2fast_api_key": ""}"""
                .trimMargin()
        val dataClass = DataClassWithCamelCase()
        assertRoundTrips(expected, dataClass, BsonConfiguration(bsonNamingStrategy = BsonNamingStrategy.SNAKE_CASE))
    }

    @Test
    fun testSameSnakeCaseName() {
        val expected = """{"my_http_auth": ""}"""
        val dataClass = DataClassWithSameSnakeCaseName()
        val exception =
            assertThrows<SerializationException> {
                assertRoundTrips(
                    expected, dataClass, BsonConfiguration(bsonNamingStrategy = BsonNamingStrategy.SNAKE_CASE))
            }
        assertEquals(
            "myHTTPAuth, myHttpAuth in org.bson.codecs.kotlinx.samples.DataClassWithSameSnakeCaseName " +
                "generate same name: my_http_auth.",
            exception.message)
    }

    @Test
    fun testKotlinAllowedName() {
        val expected = """{"имя_переменной": "", "variable _name": ""}"""
        val dataClass = DataClassWithKotlinAllowedName()
        assertRoundTrips(expected, dataClass, BsonConfiguration(bsonNamingStrategy = BsonNamingStrategy.SNAKE_CASE))
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
        println(actual.toJson())
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
        value: String,
        expected: T,
        serializersModule: SerializersModule = defaultSerializersModule,
        configuration: BsonConfiguration = BsonConfiguration()
    ) {
        assertDecodesTo(BsonDocument.parse(value), expected, serializersModule, configuration)
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
        println("Deserializing: ${value.toJson()}")
        val codec = KotlinSerializerCodec.create(T::class, serializersModule, configuration)!!
        return codec.decode(BsonDocumentReader(value), DecoderContext.builder().build())
    }
}
