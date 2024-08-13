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
package com.mongodb.kotlin.client.model

import java.util.Locale
import kotlin.test.assertEquals
import kotlinx.serialization.SerialName
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonProperty
import org.junit.Test
import org.junit.jupiter.api.assertThrows

class KPropertiesTest {

    data class Restaurant(
        @BsonId val a: String,
        @BsonProperty("b") val bsonProperty: String,
        @SerialName("c") val serialName: String,
        val name: String,
        val stringList: List<String>,
        val localeMap: Map<Locale, Review>,
        val reviews: List<Review>,
        @BsonProperty("nested") val subDocument: Restaurant?
    )

    data class Review(
        @BsonProperty("prop") val bsonProperty: String,
        @SerialName("rating") val score: String,
        val name: String,
        @BsonProperty("old") val previous: Review?
    )

    @Test
    fun testPath() {
        assertEquals("_id", Restaurant::a.path())
        assertEquals("b", Restaurant::bsonProperty.path())
        assertEquals("c", Restaurant::serialName.path())
        assertEquals("name", Restaurant::name.path())
        assertEquals("stringList", Restaurant::stringList.path())
        assertEquals("localeMap", Restaurant::localeMap.path())
        assertEquals("nested", Restaurant::subDocument.path())
        assertEquals("reviews", Restaurant::reviews.path())

        assertEquals("prop", Review::bsonProperty.path())
        assertEquals("rating", Review::score.path())
        assertEquals("name", Review::name.path())
        assertEquals("old", Review::previous.path())
    }

    @Test
    fun testDivOperator() {
        assertEquals("nested._id", (Restaurant::subDocument / Restaurant::a).path())
        assertEquals("nested.b", (Restaurant::subDocument / Restaurant::bsonProperty).path())
        assertEquals("nested.c", (Restaurant::subDocument / Restaurant::serialName).path())
        assertEquals("nested.name", (Restaurant::subDocument / Restaurant::name).path())
        assertEquals("nested.stringList", (Restaurant::subDocument / Restaurant::stringList).path())
        assertEquals("nested.localeMap", (Restaurant::subDocument / Restaurant::localeMap).path())
        assertEquals("nested.nested", (Restaurant::subDocument / Restaurant::subDocument).path())
    }

    @Test
    fun testRemOperator() {
        assertEquals("nested.prop", (Restaurant::subDocument % Review::bsonProperty).path())
        assertEquals("nested.rating", (Restaurant::subDocument % Review::score).path())
        assertEquals("nested.name", (Restaurant::subDocument % Review::name).path())
        assertEquals("nested.old", (Restaurant::subDocument % Review::previous).path())
    }

    @Test
    fun testArrayPositionalOperator() {
        assertEquals("reviews.\$", Restaurant::reviews.posOp.path())
        assertEquals("reviews.rating", (Restaurant::reviews / Review::score).path())
        assertEquals("reviews.\$.rating", (Restaurant::reviews.posOp / Review::score).path())
    }

    @Test
    fun testArrayAllPositionalOperator() {
        assertEquals("reviews.\$[]", Restaurant::reviews.allPosOp.path())
        assertEquals("reviews.\$[].rating", (Restaurant::reviews.allPosOp / Review::score).path())
    }

    @Test
    fun testArrayFilteredPositionalOperator() {
        assertEquals("reviews.\$[elem]", Restaurant::reviews.filteredPosOp("elem").path())
        assertEquals("reviews.\$[elem].rating", (Restaurant::reviews.filteredPosOp("elem") / Review::score).path())
    }

    @Test
    fun testMapProjection() {
        assertEquals("localeMap", Restaurant::localeMap.path())
        assertEquals("localeMap.en", Restaurant::localeMap.keyProjection(Locale.ENGLISH).path())
        assertEquals(
            "localeMap.en.rating", (Restaurant::localeMap.keyProjection(Locale.ENGLISH) / Review::score).path())
    }

    @Test
    fun testArrayIndexProperty() {
        assertEquals("reviews.1.rating", (Restaurant::reviews.pos(1) / Review::score).path())
    }

    @Test
    fun testKPropertyPath() {
        val property = (Restaurant::subDocument / Restaurant::a)

        assertEquals(property.annotations, Restaurant::a.annotations)
        assertEquals(property.isAbstract, Restaurant::a.isAbstract)
        assertEquals(property.isConst, Restaurant::a.isConst)
        assertEquals(property.isFinal, Restaurant::a.isFinal)
        assertEquals(property.isLateinit, Restaurant::a.isLateinit)
        assertEquals(property.isOpen, Restaurant::a.isOpen)
        assertEquals(property.isSuspend, Restaurant::a.isSuspend)
        assertEquals(property.parameters, Restaurant::a.parameters)
        assertEquals(property.returnType, Restaurant::a.returnType)
        assertEquals(property.typeParameters, Restaurant::a.typeParameters)
        assertEquals(property.visibility, Restaurant::a.visibility)

        val restaurant = Restaurant("a", "b", "c", "name", listOf(), mapOf(), listOf(), null)
        assertThrows<NotImplementedError> { property.getter }
        assertThrows<NotImplementedError> { property.invoke(restaurant) }
        assertThrows<NotImplementedError> { property.call() }
        assertThrows<NotImplementedError> { property.callBy(mapOf()) }
        assertThrows<NotImplementedError> { property.get(restaurant) }
        assertThrows<NotImplementedError> { property.getDelegate(restaurant) }
    }
}
