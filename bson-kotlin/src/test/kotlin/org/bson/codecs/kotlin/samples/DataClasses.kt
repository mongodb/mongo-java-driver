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

import org.bson.BsonMaxKey
import org.bson.BsonType
import org.bson.Document
import org.bson.codecs.pojo.annotations.BsonCreator
import org.bson.codecs.pojo.annotations.BsonDiscriminator
import org.bson.codecs.pojo.annotations.BsonExtraElements
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonIgnore
import org.bson.codecs.pojo.annotations.BsonProperty
import org.bson.codecs.pojo.annotations.BsonRepresentation

@Suppress("PropertyName", "ConstructorParameterNaming")
data class DataClass(val _id: String, val name: String, val age: Int, val hobbies: List<String>)

data class DataClassWithAnnotations(
    @BsonRepresentation(BsonType.OBJECT_ID) @BsonId val oid: String,
    @BsonProperty("nom") val name: String,
    val age: Int,
    val hobbies: List<String>
)

data class DataClassEmbedded(val name: String)

data class DataClassWithEmbedded(@BsonId val id: String, val embedded: DataClassEmbedded)

data class DataClassListOfDataClasses(@BsonId val id: String, val nested: List<DataClassEmbedded>)

data class DataClassListOfListOfDataClasses(@BsonId val id: String, val nested: List<List<DataClassEmbedded>>)

data class DataClassMapOfDataClasses(@BsonId val id: String, val nested: Map<String, DataClassEmbedded>)

data class DataClassMapOfListOfDataClasses(@BsonId val id: String, val nested: Map<String, List<DataClassEmbedded>>)

data class DataClassWithNulls(@BsonId val id: String?, val name: String, val age: Int?, val hobbies: List<String>)

data class DataClassWithDefaults(
    @BsonId val id: String,
    val name: String,
    val age: Int = 42,
    val hobbies: List<String> = listOf("computers", "databases")
)

data class DataClassSelfReferential(
    val name: String,
    val left: DataClassSelfReferential? = null,
    val right: DataClassSelfReferential? = null,
    @BsonId val id: String? = null
)

data class DataClassWithParameterizedDataClass(
    @BsonId val id: String,
    val parameterizedDataClass: DataClassParameterized<Double, DataClassEmbedded>
)

data class DataClassParameterized<N : Number, T>(val number: N, val string: String, val parameterizedList: List<T>)

data class DataClassWithNestedParameterizedDataClass(
    @BsonId val id: String,
    val nestedParameterized: DataClassWithNestedParameterized<DataClassEmbedded, String, Double>
)

data class DataClassWithNestedParameterized<A, B, C : Number>(
    val parameterizedDataClass: DataClassParameterized<C, A>,
    val other: B
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

@BsonDiscriminator data class DataClassWithBsonDiscriminator(val id: String)

data class DataClassWithBsonIgnore(val id: String, @BsonIgnore val ignored: String)

data class DataClassWithBsonExtraElements(val id: String, @BsonExtraElements val extraElements: Document)

data class DataClassWithBsonConstructor(val id: String, val count: Int) {
    @BsonCreator constructor(id: String) : this(id, -1)
}

data class DataClassWithInvalidRepresentation(@BsonRepresentation(BsonType.STRING) val id: BsonMaxKey)

data class DataClassWithFailingInit(@BsonId val id: String) {
    init {
        require(false)
    }
}

data class DataClassWithSequence(val value: Sequence<String>)
