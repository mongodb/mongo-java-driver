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
package org.bson.codecs.kotlin.samples

import kotlin.time.Duration
import org.bson.BsonDocument
import org.bson.BsonMaxKey
import org.bson.BsonType
import org.bson.codecs.pojo.annotations.BsonCreator
import org.bson.codecs.pojo.annotations.BsonDiscriminator
import org.bson.codecs.pojo.annotations.BsonExtraElements
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonIgnore
import org.bson.codecs.pojo.annotations.BsonProperty
import org.bson.codecs.pojo.annotations.BsonRepresentation
import org.bson.types.ObjectId

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

data class DataClassWithCollections(
    val listSimple: List<String>,
    val listList: List<List<String>>,
    val listMap: List<Map<String, Int>>,
    val mapSimple: Map<String, Int>,
    val mapList: Map<String, List<String>>,
    val mapMap: Map<String, Map<String, Int>>
)

data class DataClassWithArrays(
    val arraySimple: Array<String>,
    val nestedArrays: Array<Array<String>>,
    val arrayOfMaps: Array<Map<String, Array<String>>>
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DataClassWithArrays

        if (!arraySimple.contentEquals(other.arraySimple)) return false
        if (!nestedArrays.contentDeepEquals(other.nestedArrays)) return false

        if (arrayOfMaps.size != other.arrayOfMaps.size) return false
        arrayOfMaps.forEachIndexed { i, map ->
            val otherMap = other.arrayOfMaps[i]
            if (map.keys != otherMap.keys) return false
            map.keys.forEach { key -> if (!map[key].contentEquals(otherMap[key])) return false }
        }

        return true
    }

    override fun hashCode(): Int {
        var result = arraySimple.contentHashCode()
        result = 31 * result + nestedArrays.contentDeepHashCode()
        result = 31 * result + arrayOfMaps.contentHashCode()
        return result
    }
}

data class DataClassWithNativeArrays(
    val booleanArray: BooleanArray,
    val byteArray: ByteArray,
    val charArray: CharArray,
    val doubleArray: DoubleArray,
    val floatArray: FloatArray,
    val intArray: IntArray,
    val longArray: LongArray,
    val shortArray: ShortArray,
    val listOfArrays: List<BooleanArray>,
    val mapOfArrays: Map<String, IntArray>
) {

    @SuppressWarnings("ComplexMethod")
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DataClassWithNativeArrays

        if (!booleanArray.contentEquals(other.booleanArray)) return false
        if (!byteArray.contentEquals(other.byteArray)) return false
        if (!charArray.contentEquals(other.charArray)) return false
        if (!doubleArray.contentEquals(other.doubleArray)) return false
        if (!floatArray.contentEquals(other.floatArray)) return false
        if (!intArray.contentEquals(other.intArray)) return false
        if (!longArray.contentEquals(other.longArray)) return false
        if (!shortArray.contentEquals(other.shortArray)) return false

        if (listOfArrays.size != other.listOfArrays.size) return false
        listOfArrays.forEachIndexed { i, value -> if (!value.contentEquals(other.listOfArrays[i])) return false }

        if (mapOfArrays.keys != other.mapOfArrays.keys) return false
        mapOfArrays.keys.forEach { key -> if (!mapOfArrays[key].contentEquals(other.mapOfArrays[key])) return false }

        return true
    }

    override fun hashCode(): Int {
        var result = booleanArray.contentHashCode()
        result = 31 * result + byteArray.contentHashCode()
        result = 31 * result + charArray.contentHashCode()
        result = 31 * result + doubleArray.contentHashCode()
        result = 31 * result + floatArray.contentHashCode()
        result = 31 * result + intArray.contentHashCode()
        result = 31 * result + longArray.contentHashCode()
        result = 31 * result + shortArray.contentHashCode()
        result = 31 * result + listOfArrays.hashCode()
        result = 31 * result + mapOfArrays.hashCode()
        return result
    }
}

data class DataClassWithDefaults(
    val boolean: Boolean = false,
    val string: String = "String",
    val listSimple: List<String> = listOf("a", "b", "c")
)

data class DataClassWithNulls(val boolean: Boolean?, val string: String?, val listSimple: List<String?>?)

data class DataClassWithListThatLastItemDefaultsToNull(val elements: List<DataClassLastItemDefaultsToNull>)

data class DataClassLastItemDefaultsToNull(val required: String, val optional: String? = null)

data class DataClassSelfReferential(
    val name: String,
    val left: DataClassSelfReferential? = null,
    val right: DataClassSelfReferential? = null
)

data class DataClassEmbedded(val name: String)

data class DataClassWithEmbedded(val id: String, val embedded: DataClassEmbedded)

data class DataClassListOfDataClasses(val id: String, val nested: List<DataClassEmbedded>)

data class DataClassListOfListOfDataClasses(val id: String, val nested: List<List<DataClassEmbedded>>)

data class DataClassMapOfDataClasses(val id: String, val nested: Map<String, DataClassEmbedded>)

data class DataClassMapOfListOfDataClasses(val id: String, val nested: Map<String, List<DataClassEmbedded>>)

data class DataClassWithParameterizedDataClass(
    val id: String,
    val parameterizedDataClass: DataClassParameterized<Double, DataClassEmbedded>
)

data class DataClassParameterized<N : Number, T>(val number: N, val string: String, val parameterizedList: List<T>)

data class DataClassWithNestedParameterizedDataClass(
    val id: String,
    val nestedParameterized: DataClassWithNestedParameterized<DataClassEmbedded, String, Double>
)

data class DataClassWithNestedParameterized<A, B, C : Number>(
    val parameterizedDataClass: DataClassParameterized<C, A>,
    val other: B,
    val optionalOther: B?
)

data class DataClassWithPair(val pair: Pair<String, Int>)

data class DataClassWithTriple(val triple: Triple<String, Int, Double>)

data class DataClassNestedParameterizedTypes(
    val triple:
        Triple<
            String,
            Pair<Int, Pair<Double, Pair<String, Double>>>,
            Triple<Int, Pair<Double, String>, Triple<String, Pair<Double, String>, Double>>>
)

data class DataClassWithMutableList(val value: MutableList<String>)

data class DataClassWithMutableSet(val value: MutableSet<String>)

data class DataClassWithMutableMap(val value: MutableMap<String, String>)

data class DataClassWithBooleanMapKey(val map: Map<Boolean, Boolean>)

enum class Key {
    A,
    B
}

data class DataClassWithEnum(val value: Key)

data class DataClassWithEnumMapKey(val map: Map<Key, Boolean>)

data class DataClassKey(val value: String)

data class DataClassWithDataClassMapKey(val map: Map<DataClassKey, Boolean>)

data class DataClassWithObjectIdAndBsonDocument(val objectId: ObjectId, val bsonDocument: BsonDocument)

sealed class DataClassSealed

data class DataClassSealedA(val a: String) : DataClassSealed()

data class DataClassSealedB(val b: Int) : DataClassSealed()

data class DataClassSealedC(val c: String) : DataClassSealed()

data class DataClassListOfSealed(val items: List<DataClassSealed>)

data class DataClassWithBsonId(@BsonId val id: String)

data class DataClassWithBsonProperty(@BsonProperty("_id") val id: String)

@BsonDiscriminator data class DataClassWithBsonDiscriminator(val id: String)

data class DataClassWithBsonIgnore(val id: String, @BsonIgnore val ignored: String)

data class DataClassWithBsonExtraElements(val id: String, @BsonExtraElements val extraElements: Map<String, String>)

data class DataClassWithBsonConstructor(val id: String, val count: Int) {
    @BsonCreator constructor(id: String) : this(id, -1)
}

data class DataClassWithInvalidBsonRepresentation(@BsonRepresentation(BsonType.STRING) val id: BsonMaxKey)

data class DataClassWithFailingInit(val id: String) {
    init {
        require(false)
    }
}

data class DataClassWithSequence(val value: Sequence<String>)

data class DataClassWithJVMErasure(val duration: Duration, val ints: List<Int>)

data class Box<T>(val boxed: T)

data class DataClassWithNullableGeneric(val box: Box<String?>)
