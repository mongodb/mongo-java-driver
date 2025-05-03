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
package org.bson.codecs.kotlinx.samples

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.serialization.Contextual
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Required
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonJavaScriptWithScope
import org.bson.BsonMaxKey
import org.bson.BsonMinKey
import org.bson.BsonObjectId
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.BsonType
import org.bson.BsonUndefined
import org.bson.codecs.pojo.annotations.BsonCreator
import org.bson.codecs.pojo.annotations.BsonDiscriminator
import org.bson.codecs.pojo.annotations.BsonExtraElements
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonIgnore
import org.bson.codecs.pojo.annotations.BsonProperty
import org.bson.codecs.pojo.annotations.BsonRepresentation
import org.bson.types.ObjectId

@Serializable
data class DataClassWithSimpleValues(
    val char: Char,
    val byte: Byte,
    val short: Short,
    val int: Int,
    val long: Long,
    val float: Float,
    val double: Double,
    val boolean: Boolean,
    val string: String
)

@Serializable
data class DataClassWithContextualDateValues(
    @Contextual val instant: Instant,
    @Contextual val localTime: LocalTime,
    @Contextual val localDateTime: LocalDateTime,
    @Contextual val localDate: LocalDate,
)

@Serializable
data class DataClassWithDateValues(
    val instant: Instant,
    val localTime: LocalTime,
    val localDateTime: LocalDateTime,
    val localDate: LocalDate,
)

@Serializable
data class DataClassWithCollections(
    val listSimple: List<String>,
    val listList: List<List<String>>,
    val listMap: List<Map<String, Int>>,
    val mapSimple: Map<String, Int>,
    val mapList: Map<String, List<String>>,
    val mapMap: Map<String, Map<String, Int>>
)

@Serializable
data class DataClassWithDefaults(
    val boolean: Boolean = false,
    val string: String = "String",
    val listSimple: List<String> = listOf("a", "b", "c")
)

@Serializable
data class DataClassWithCamelCase(
    val twoWords: String = "",
    @Suppress("ConstructorParameterNaming") val MyProperty: String = "",
    @Suppress("ConstructorParameterNaming") val camel_Case_Underscores: String = "",
    @Suppress("ConstructorParameterNaming") val URLMapping: String = "",
    val myHTTPAuth: String = "",
    val myHTTP2ApiKey: String = "",
    val myHTTP2fastApiKey: String = "",
)

@Serializable
data class DataClassWithSameSnakeCaseName(
    val myHTTPAuth: String = "",
    val myHttpAuth: String = "",
    val myHTTPAuth1: String = "",
    val myHttpAuth1: String = "",
)

@Serializable
data class DataClassWithKotlinAllowedName(
    @Suppress("ConstructorParameterNaming") val имяПеременной: String = "",
    @Suppress("ConstructorParameterNaming") val `variable Name`: String = "",
)

@Serializable data class DataClassWithNulls(val boolean: Boolean?, val string: String?, val listSimple: List<String?>?)

@Serializable
data class DataClassWithListThatLastItemDefaultsToNull(val elements: List<DataClassLastItemDefaultsToNull>)

@Serializable data class DataClassLastItemDefaultsToNull(val required: String, val optional: String? = null)

@Serializable
data class DataClassSelfReferential(
    val name: String,
    val left: DataClassSelfReferential? = null,
    val right: DataClassSelfReferential? = null
)

@Serializable data class DataClassEmbedded(val name: String)

@Serializable data class DataClassWithEmbedded(val id: String, val embedded: DataClassEmbedded)

@Serializable data class DataClassListOfDataClasses(val id: String, val nested: List<DataClassEmbedded>)

@Serializable data class DataClassListOfListOfDataClasses(val id: String, val nested: List<List<DataClassEmbedded>>)

@Serializable data class DataClassMapOfDataClasses(val id: String, val nested: Map<String, DataClassEmbedded>)

@Serializable
data class DataClassMapOfListOfDataClasses(val id: String, val nested: Map<String, List<DataClassEmbedded>>)

@Serializable
data class DataClassWithParameterizedDataClass(
    val id: String,
    val parameterizedDataClass: DataClassParameterized<Double, DataClassEmbedded>
)

@Serializable
data class DataClassParameterized<N : Number, T>(val number: N, val string: String, val parameterizedList: List<T>)

@Serializable
data class DataClassWithNestedParameterizedDataClass(
    val id: String,
    val nestedParameterized: DataClassWithNestedParameterized<DataClassEmbedded, String, Double>
)

@Serializable
data class DataClassWithNestedParameterized<A, B, C : Number>(
    val parameterizedDataClass: DataClassParameterized<C, A>,
    val other: B,
    val optionalOther: B?
)

@Serializable data class DataClassWithPair(val pair: Pair<String, Int>)

@Serializable data class DataClassWithTriple(val triple: Triple<String, Int, Double>)

@Serializable
data class DataClassNestedParameterizedTypes(
    val triple:
        Triple<
            String,
            Pair<Int, Pair<Double, Pair<String, Double>>>,
            Triple<Int, Pair<Double, String>, Triple<String, Pair<Double, String>, Double>>>
)

@Serializable data class DataClassWithMutableList(val value: MutableList<String>)

@Serializable data class DataClassWithMutableSet(val value: MutableSet<String>)

@Serializable data class DataClassWithMutableMap(val value: MutableMap<String, String>)

@Serializable
data class DataClassWithAnnotations(
    @SerialName("_id") val id: String,
    @SerialName("nom") val name: String,
    val string: String
)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class DataClassWithEncodeDefault(
    val boolean: Boolean = false,
    @EncodeDefault(EncodeDefault.Mode.NEVER) val string: String = "String",
    @EncodeDefault(EncodeDefault.Mode.ALWAYS) val listSimple: List<String> = listOf("a", "b", "c")
)

@Serializable
data class DataClassWithRequired(
    val boolean: Boolean = false,
    @Required val string: String = "String",
    @Required val listSimple: List<String> = listOf("a", "b", "c")
)

@Serializable data class DataClassWithBooleanMapKey(val map: Map<Boolean, Boolean>)

enum class Key {
    A,
    B
}

@Serializable data class DataClassWithEnum(val value: Key)

@Serializable data class DataClassWithEnumMapKey(val map: Map<Key, Boolean>)

@Serializable data class DataClassKey(val value: String)

@Serializable data class DataClassWithDataClassMapKey(val map: Map<DataClassKey, Boolean>)

@Serializable
data class DataClassBsonValues(
    @Contextual val id: ObjectId,
    @Contextual val arrayEmpty: BsonArray,
    @Contextual val arraySimple: BsonArray,
    @Contextual val arrayComplex: BsonArray,
    @Contextual val arrayMixedTypes: BsonArray,
    @Contextual val arrayComplexMixedTypes: BsonArray,
    @Contextual val binary: BsonBinary,
    @Contextual val boolean: BsonBoolean,
    @Contextual val code: BsonJavaScript,
    @Contextual val codeWithScope: BsonJavaScriptWithScope,
    @Contextual val dateTime: BsonDateTime,
    @Contextual val decimal128: BsonDecimal128,
    @Contextual val documentEmpty: BsonDocument,
    @Contextual val document: BsonDocument,
    @Contextual val double: BsonDouble,
    @Contextual val int32: BsonInt32,
    @Contextual val int64: BsonInt64,
    @Contextual val maxKey: BsonMaxKey,
    @Contextual val minKey: BsonMinKey,
    @Contextual val objectId: BsonObjectId,
    @Contextual val regex: BsonRegularExpression,
    @Contextual val string: BsonString,
    @Contextual val symbol: BsonSymbol,
    @Contextual val timestamp: BsonTimestamp,
    @Contextual val undefined: BsonUndefined,
)

@Serializable
data class DataClassOptionalBsonValues(
    @Contextual val id: ObjectId?,
    @Contextual val arrayEmpty: BsonArray?,
    @Contextual val arraySimple: BsonArray?,
    @Contextual val arrayComplex: BsonArray?,
    @Contextual val arrayMixedTypes: BsonArray?,
    @Contextual val arrayComplexMixedTypes: BsonArray?,
    @Contextual val binary: BsonBinary?,
    @Contextual val boolean: BsonBoolean?,
    @Contextual val code: BsonJavaScript?,
    @Contextual val codeWithScope: BsonJavaScriptWithScope?,
    @Contextual val dateTime: BsonDateTime?,
    @Contextual val decimal128: BsonDecimal128?,
    @Contextual val documentEmpty: BsonDocument?,
    @Contextual val document: BsonDocument?,
    @Contextual val double: BsonDouble?,
    @Contextual val int32: BsonInt32?,
    @Contextual val int64: BsonInt64?,
    @Contextual val maxKey: BsonMaxKey?,
    @Contextual val minKey: BsonMinKey?,
    @Contextual val objectId: BsonObjectId?,
    @Contextual val regex: BsonRegularExpression?,
    @Contextual val string: BsonString?,
    @Contextual val symbol: BsonSymbol?,
    @Contextual val timestamp: BsonTimestamp?,
    @Contextual val undefined: BsonUndefined?,
)

@Serializable sealed class DataClassSealed

@Serializable data class DataClassSealedA(val a: String) : DataClassSealed()

@Serializable data class DataClassSealedB(val b: Int) : DataClassSealed()

@Serializable @SerialName("C") data class DataClassSealedC(val c: String) : DataClassSealed()

@Serializable
sealed interface SealedInterface {
    val name: String
}

@Serializable
data class DataClassSealedInterface(@Contextual @SerialName("_id") val id: ObjectId, override val name: String) :
    SealedInterface

@Serializable data class DataClassListOfSealed(val items: List<DataClassSealed>)

interface DataClassOpen

@Serializable data class DataClassOpenA(val a: String) : DataClassOpen

@Serializable data class DataClassOpenB(val b: Int) : DataClassOpen

@Serializable data class DataClassContainsOpen(val open: DataClassOpen)

@JvmInline @Serializable value class ValueClass(val s: String)

@Serializable data class DataClassContainsValueClass(val value: ValueClass)

@Serializable data class DataClassWithBsonId(@BsonId val id: String)

@Serializable data class DataClassWithBsonProperty(@BsonProperty("_id") val id: String)

@BsonDiscriminator @Serializable data class DataClassWithBsonDiscriminator(val id: String)

@Serializable data class DataClassWithBsonIgnore(val id: String, @BsonIgnore val ignored: String)

@Serializable
data class DataClassWithBsonExtraElements(val id: String, @BsonExtraElements val extraElements: Map<String, String>)

@Serializable
data class DataClassWithBsonConstructor(val id: String, val count: Int) {
    @BsonCreator constructor(id: String) : this(id, -1)
}

@Serializable data class DataClassWithBsonRepresentation(@BsonRepresentation(BsonType.STRING) val id: Int)

@Serializable
data class DataClassWithFailingInit(val id: String) {
    init {
        require(false)
    }
}

@Serializable data class DataClassWithSequence(val value: Sequence<String>)

@Serializable data class Box<T>(val boxed: T)

@Serializable data class DataClassWithNullableGeneric(val box: Box<String?>)

@Serializable data class DataClassWithJsonElement(val value: JsonElement)

@Serializable
data class DataClassWithJsonElements(
    val jsonElement: JsonElement,
    val jsonArray: JsonArray,
    val jsonElements: List<JsonElement>,
    val jsonNestedMap: Map<String, JsonElement>
)

@Serializable
data class DataClassWithJsonElementsNullable(
    val jsonElement: JsonElement?,
    val jsonArray: JsonArray?,
    val jsonElements: List<JsonElement?>?,
    val jsonNestedMap: Map<String, JsonElement?>?
)
