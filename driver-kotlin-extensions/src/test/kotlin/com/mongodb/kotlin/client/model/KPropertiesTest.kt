/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (C) 2016/2022 Litote
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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.serialization.SerialName
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonProperty
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
        @BsonProperty("old") val previous: Review?,
        @BsonProperty("nested") val misc: List<String>
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
        assertEquals("reviews.nested.\$", (Restaurant::reviews / Review::misc).posOp.path())
        assertEquals("reviews.\$.rating", (Restaurant::reviews.posOp / Review::score).path())
    }

    @Test
    fun testArrayAllPositionalOperator() {
        assertEquals("reviews.\$[]", Restaurant::reviews.allPosOp.path())
        assertEquals("reviews.\$[].rating", (Restaurant::reviews.allPosOp / Review::score).path())
        assertEquals("reviews.nested.\$[]", (Restaurant::reviews / Review::misc).allPosOp.path())
    }

    @Test
    fun testArrayFilteredPositionalOperator() {
        assertEquals("reviews.\$[elem]", Restaurant::reviews.filteredPosOp("elem").path())
        assertEquals("reviews.\$[elem].rating", (Restaurant::reviews.filteredPosOp("elem") / Review::score).path())
    }

    @Test
    fun testMapProjection() {
        assertEquals("localeMap", Restaurant::localeMap.path())
        assertEquals("localeMap.rating", (Restaurant::localeMap / Review::score).path())
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
        assertThrows<UnsupportedOperationException> { property.annotations }
        assertThrows<UnsupportedOperationException> { property.isAbstract }
        assertThrows<UnsupportedOperationException> { property.isConst }
        assertThrows<UnsupportedOperationException> { property.isFinal }
        assertThrows<UnsupportedOperationException> { property.isLateinit }
        assertThrows<UnsupportedOperationException> { property.isOpen }
        assertThrows<UnsupportedOperationException> { property.isSuspend }
        assertThrows<UnsupportedOperationException> { property.parameters }
        assertThrows<UnsupportedOperationException> { property.returnType }
        assertThrows<UnsupportedOperationException> { property.typeParameters }
        assertThrows<UnsupportedOperationException> { property.visibility }

        val restaurant = Restaurant("a", "b", "c", "name", listOf(), mapOf(), listOf(), null)
        assertThrows<UnsupportedOperationException> { property.getter }
        assertThrows<UnsupportedOperationException> { property.invoke(restaurant) }
        assertThrows<UnsupportedOperationException> { property.call() }
        assertThrows<UnsupportedOperationException> { property.callBy(mapOf()) }
        assertThrows<UnsupportedOperationException> { property.get(restaurant) }
        assertThrows<UnsupportedOperationException> { property.getDelegate(restaurant) }
    }

    @Test
    fun testNoCacheCollisions() {
        for (i in 1.rangeTo(25_000)) {
            assertEquals("reviews.$i", Restaurant::reviews.pos(i).path())
            assertEquals("reviews.$[identifier$i]",  Restaurant::reviews.filteredPosOp("identifier$i").path())
            assertEquals("localeMap.$i", Restaurant::localeMap.keyProjection(i).path())

            val x = i / 2
            assertEquals("reviews.$[identifier$x].rating", (Restaurant::reviews.filteredPosOp("identifier$x") / Review::score).path())
            assertEquals("reviews.$x.rating", (Restaurant::reviews.pos(x) / Review::score).path())
            assertEquals("localeMap.$x.rating", (Restaurant::localeMap.keyProjection(x) / Review::score).path())
        }
    }
}
