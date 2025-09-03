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
 *
 * @custom-license-header
 */
package com.mongodb.kotlin.client.model

import com.mongodb.MongoClientSettings
import com.mongodb.client.model.TextSearchOptions
import com.mongodb.client.model.geojson.Point
import com.mongodb.client.model.geojson.Polygon
import com.mongodb.client.model.geojson.Position
import com.mongodb.kotlin.client.model.Filters.all
import com.mongodb.kotlin.client.model.Filters.and
import com.mongodb.kotlin.client.model.Filters.bitsAllClear
import com.mongodb.kotlin.client.model.Filters.bitsAllSet
import com.mongodb.kotlin.client.model.Filters.bitsAnyClear
import com.mongodb.kotlin.client.model.Filters.bitsAnySet
import com.mongodb.kotlin.client.model.Filters.elemMatch
import com.mongodb.kotlin.client.model.Filters.eq
import com.mongodb.kotlin.client.model.Filters.exists
import com.mongodb.kotlin.client.model.Filters.expr
import com.mongodb.kotlin.client.model.Filters.geoIntersects
import com.mongodb.kotlin.client.model.Filters.geoWithin
import com.mongodb.kotlin.client.model.Filters.geoWithinBox
import com.mongodb.kotlin.client.model.Filters.geoWithinCenter
import com.mongodb.kotlin.client.model.Filters.geoWithinCenterSphere
import com.mongodb.kotlin.client.model.Filters.geoWithinPolygon
import com.mongodb.kotlin.client.model.Filters.gt
import com.mongodb.kotlin.client.model.Filters.gte
import com.mongodb.kotlin.client.model.Filters.`in`
import com.mongodb.kotlin.client.model.Filters.jsonSchema
import com.mongodb.kotlin.client.model.Filters.lt
import com.mongodb.kotlin.client.model.Filters.lte
import com.mongodb.kotlin.client.model.Filters.mod
import com.mongodb.kotlin.client.model.Filters.ne
import com.mongodb.kotlin.client.model.Filters.near
import com.mongodb.kotlin.client.model.Filters.nearSphere
import com.mongodb.kotlin.client.model.Filters.nin
import com.mongodb.kotlin.client.model.Filters.nor
import com.mongodb.kotlin.client.model.Filters.not
import com.mongodb.kotlin.client.model.Filters.or
import com.mongodb.kotlin.client.model.Filters.regex
import com.mongodb.kotlin.client.model.Filters.size
import com.mongodb.kotlin.client.model.Filters.text
import com.mongodb.kotlin.client.model.Filters.type
import com.mongodb.kotlin.client.model.Filters.where
import kotlin.test.Test
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.BsonType
import org.bson.conversions.Bson

class FiltersTest {

    data class Person(val name: String, val age: Int, val address: List<String>, val results: List<Int>)
    val person = Person("Ada", 20, listOf("St James Square", "London", "W1"), listOf(1, 2, 3))

    @Test
    fun testEqSupport() {
        val expected = BsonDocument.parse("""{"name": "Ada"}""")
        val bson = eq(Person::name, person.name)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::name eq person.name
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testNeSupport() {
        val expected = BsonDocument.parse("""{"age": {"${'$'}ne": 20 }}""")
        val bson = ne(Person::age, person.age)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::age ne person.age
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testNotSupport() {
        val expected = BsonDocument.parse("""{"age": {${'$'}not: {${'$'}eq: 20 }}}""")
        val bson = not(eq(Person::age, person.age))
        assertEquals(expected, bson.document)

        val kmongoDsl = not(Person::age eq person.age)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testGtSupport() {
        val expected = BsonDocument.parse("""{"age": {"${'$'}gt": 20}}""")
        val bson = gt(Person::age, person.age)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::age gt 20
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testGteSupport() {
        val expected = BsonDocument.parse("""{"age": {"${'$'}gte": 20}}""")
        val bson = gte(Person::age, person.age)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::age gte 20
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testLtSupport() {
        val expected = BsonDocument.parse("""{"age": {"${'$'}lt": 20}}""")
        val bson = lt(Person::age, person.age)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::age lt 20
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testLteSupport() {
        val expected = BsonDocument.parse("""{"age": {"${'$'}lte": 20}}""")
        val bson = lte(Person::age, person.age)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::age lte 20
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testExistsSupport() {
        val expected = BsonDocument.parse("""{"age": {"${'$'}exists": true}}""")

        var bson = exists(Person::age)
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::age.exists()
        assertEquals(expected, kmongoDsl.document)

        bson = exists(Person::age, true)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::age exists true
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testOrSupport() {
        val expected = BsonDocument.parse("""{${'$'}or: [{"name": "Ada"}, {"age": 20 }]}""")
        val bson = or(eq(Person::name, person.name), eq(Person::age, person.age))
        assertEquals(expected, bson.document)

        val kmongoDsl = or(Person::name eq person.name, Person::age eq person.age)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testNorSupport() {
        val expected = BsonDocument.parse("""{${'$'}nor: [{"name": "Ada"}, {"age": 20 }]}""")
        var bson = nor(eq(Person::name, person.name), eq(Person::age, person.age))
        assertEquals(expected, bson.document)

        var kmongoDsl = nor(Person::name eq person.name, Person::age eq person.age)
        assertEquals(expected, kmongoDsl.document)

        // List api
        bson = nor(listOf(eq(Person::name, person.name), eq(Person::age, person.age)))
        assertEquals(expected, bson.document)

        kmongoDsl = nor(listOf(Person::name eq person.name, Person::age eq person.age))
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testAndSupport() {
        val expected = BsonDocument.parse("""{${'$'}and: [{"name": "Ada"}, {"age": 20 }]}""")
        val bson = and(eq(Person::name, person.name), eq(Person::age, person.age))
        assertEquals(expected, bson.document)

        val kmongoDsl = and(Person::name.eq(person.name), Person::age.eq(person.age))
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testAllSupport() {
        val expected = BsonDocument.parse("""{"address": {${'$'}all: ["a", "b", "c"]}}""")
        var bson = all(Person::address, "a", "b", "c")
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::address.all("a", "b", "c")
        assertEquals(expected, kmongoDsl.document)

        bson = all(Person::address, listOf("a", "b", "c"))
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.all(listOf("a", "b", "c"))
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testElemMatchSupport() {
        val expected =
            BsonDocument.parse(
                """{"results": {"${'$'}elemMatch":
            |{"${'$'}and": [{"age": {"${'$'}gt": 1}}, {"age": {"${'$'}lt": 10}}]}}}"""
                    .trimMargin())
        val bson = elemMatch(Person::results, and(gt(Person::age, 1), lt(Person::age, 10)))
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::results elemMatch and(gt(Person::age, 1), lt(Person::age, 10))
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testInSupport() {
        // List of values
        var expected = BsonDocument.parse("""{"results": {"${'$'}in": [1, 2, 3]}}""")
        var bson = `in`(Person::results, person.results)
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::results.`in`(person.results)
        assertEquals(expected, kmongoDsl.document)

        // Alternative implementations
        expected = BsonDocument.parse("""{"name": {"${'$'}in": ["Abe", "Ada", "Asda"]}}""")
        bson = `in`(Person::name, listOf("Abe", "Ada", "Asda"))
        assertEquals(expected, bson.document)

        kmongoDsl = Person::name.`in`(listOf("Abe", "Ada", "Asda"))
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testNinSupport() {
        // List of values
        var expected = BsonDocument.parse("""{"results": {"${'$'}nin": [1, 2, 3]}}""")
        var bson = nin(Person::results, person.results)
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::results.nin(person.results)
        assertEquals(expected, kmongoDsl.document)

        // Alternative implementations
        expected = BsonDocument.parse("""{"name": {"${'$'}nin": ["Abe", "Ada", "Asda"]}}""")
        bson = nin(Person::name, listOf("Abe", "Ada", "Asda"))
        assertEquals(expected, bson.document)

        kmongoDsl = Person::name.nin(listOf("Abe", "Ada", "Asda"))
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testModSupport() {
        val expected =
            BsonDocument.parse(
                """{"age": {"${'$'}mod": [{ "${'$'}numberLong" : "20" }, { "${'$'}numberLong" : "0" }]}}""")
        val bson = mod(Person::age, person.age.toLong(), 0)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::age.mod(person.age.toLong(), 0)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testSizeSupport() {
        val expected = BsonDocument.parse("""{"results": {"${'$'}size": 3}}""")
        val bson = size(Person::results, 3)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::results.size(3)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testBitsAllClearSupport() {
        val expected = BsonDocument.parse("""{"results": {"${'$'}bitsAllClear":  { "${'$'}numberLong" : "3" }}}""")
        val bson = bitsAllClear(Person::results, 3)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::results bitsAllClear 3
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testBitsSetClearSupport() {
        // List of values
        val expected = BsonDocument.parse("""{"results": {"${'$'}bitsAllSet":  { "${'$'}numberLong" : "3" }}}""")
        val bson = bitsAllSet(Person::results, 3)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::results bitsAllSet 3
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testBitsAnyClearSupport() {
        val expected = BsonDocument.parse("""{"results": {"${'$'}bitsAnyClear":  { "${'$'}numberLong" : "3" }}}""")
        val bson = bitsAnyClear(Person::results, 3)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::results bitsAnyClear 3
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testBitsAnySetSupport() {
        val expected = BsonDocument.parse("""{"results": {"${'$'}bitsAnySet":  { "${'$'}numberLong" : "3" }}}""")
        val bson = bitsAnySet(Person::results, 3)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::results bitsAnySet 3
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testTypeSupport() {
        val expected = BsonDocument.parse("""{"results": {"${'$'}type":  5}}""")
        val bson = type(Person::results, BsonType.BINARY)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::results type BsonType.BINARY
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testTextSupport() {
        var expected = BsonDocument.parse("""{${'$'}text: {${'$'}search: "mongoDB for GIANT ideas"}}""")
        var bson = text("mongoDB for GIANT ideas")
        assertEquals(expected, bson.document)

        expected =
            BsonDocument.parse("""{${'$'}text: {${'$'}search: "mongoDB for GIANT ideas", ${'$'}language: "english"}}""")
        bson = text("mongoDB for GIANT ideas", TextSearchOptions().language("english"))
        assertEquals(expected, bson.document)
    }

    @Test
    fun testRegexSupport() {
        val pattern = "acme.*corp"
        var expected = BsonDocument.parse("""{"name": {"${'$'}regex": "$pattern", ${'$'}options : ""}}}""")
        var bson = regex(Person::name, pattern)
        assertEquals(expected, bson.document)

        bson = regex(Person::name, pattern.toRegex())
        assertEquals(expected, bson.document)

        bson = regex(Person::name, pattern.toRegex().toPattern())
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::name.regex(pattern)
        assertEquals(expected, kmongoDsl.document)

        kmongoDsl = Person::name.regex(pattern.toRegex())
        assertEquals(expected, kmongoDsl.document)

        kmongoDsl = Person::name.regex(pattern.toRegex().toPattern())
        assertEquals(expected, kmongoDsl.document)

        // With options
        val options = "iu"
        expected = BsonDocument.parse("""{"name": {"${'$'}regex": "$pattern", ${'$'}options : "$options"}}}""")
        bson = regex(Person::name, pattern, options)
        assertEquals(expected, bson.document)

        bson = regex(Person::name, pattern.toRegex(RegexOption.IGNORE_CASE))
        assertEquals(expected, bson.document)

        bson = regex(Person::name, pattern.toRegex(RegexOption.IGNORE_CASE).toPattern())
        assertEquals(expected, bson.document)

        kmongoDsl = Person::name.regex(pattern, options)
        assertEquals(expected, kmongoDsl.document)

        kmongoDsl = Person::name.regex(pattern.toRegex(RegexOption.IGNORE_CASE))
        assertEquals(expected, kmongoDsl.document)

        kmongoDsl = Person::name.regex(pattern.toRegex(RegexOption.IGNORE_CASE).toPattern())
        assertEquals(expected, kmongoDsl.document)

        // Iterable<String?>
        expected = BsonDocument.parse("""{"address": {"${'$'}regex": "$pattern", ${'$'}options : ""}}}""")
        bson = regex(Person::address, pattern)
        assertEquals(expected, bson.document)

        bson = regex(Person::address, pattern.toRegex())
        assertEquals(expected, bson.document)

        bson = regex(Person::address, pattern.toRegex().toPattern())
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.regex(pattern)
        assertEquals(expected, kmongoDsl.document)

        kmongoDsl = Person::address.regex(pattern.toRegex())
        assertEquals(expected, kmongoDsl.document)

        kmongoDsl = Person::address.regex(pattern.toRegex().toPattern())
        assertEquals(expected, kmongoDsl.document)

        expected = BsonDocument.parse("""{"address": {"${'$'}regex": "$pattern", ${'$'}options : "$options"}}}""")
        bson = regex(Person::address, pattern, options)
        assertEquals(expected, bson.document)

        bson = regex(Person::address, pattern.toRegex(RegexOption.IGNORE_CASE))
        assertEquals(expected, bson.document)

        bson = regex(Person::address, pattern.toRegex(RegexOption.IGNORE_CASE).toPattern())
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.regex(pattern, options)
        assertEquals(expected, kmongoDsl.document)

        kmongoDsl = Person::address.regex(pattern.toRegex(RegexOption.IGNORE_CASE))
        assertEquals(expected, kmongoDsl.document)

        kmongoDsl = Person::address.regex(pattern.toRegex(RegexOption.IGNORE_CASE).toPattern())
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testWhereSupport() {
        val expected = BsonDocument.parse("""{${'$'}where: "this.address.0 == this.address.1"}""")
        val bson = where("this.address.0 == this.address.1")

        assertEquals(expected, bson.document)
    }

    @Test
    fun testExprSupport() {
        val expected = BsonDocument.parse("""{${'$'}expr: {"name": "Ada"}}""")
        val bson = expr(Person::name eq person.name)

        assertEquals(expected, bson.document)
    }

    @Test
    fun testGeoWithinSupport() {
        val geometry =
            """{"${'$'}geometry":  {"type": "Polygon",
                | "coordinates": [[[1.0, 2.0], [2.0, 3.0], [3.0, 4.0], [1.0, 2.0]]]}}"""
                .trimMargin()
        val expected = BsonDocument.parse("""{"address": {"${'$'}geoWithin": $geometry}}""")
        val polygon = Polygon(listOf(Position(1.0, 2.0), Position(2.0, 3.0), Position(3.0, 4.0), Position(1.0, 2.0)))
        var bson = geoWithin(Person::address, polygon)
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::address geoWithin polygon
        assertEquals(expected, kmongoDsl.document)

        // Using Bson
        val bsonGeometry = BsonDocument.parse(geometry).getDocument("${'$'}geometry")
        bson = geoWithin(Person::address, bsonGeometry)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address geoWithin bsonGeometry
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testGeoWithinBoxSupport() {
        val expected =
            BsonDocument.parse("""{"address": {"${'$'}geoWithin": {"${'$'}box":  [[1.0, 2.0], [3.0, 4.0]]}}}}""")
        val bson = geoWithinBox(Person::address, 1.0, 2.0, 3.0, 4.0)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::address.geoWithinBox(1.0, 2.0, 3.0, 4.0)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testGeoWithinPolygonSupport() {
        val expected =
            BsonDocument.parse(
                """{"address": {"${'$'}geoWithin": {"${'$'}polygon":  [[0.0, 0.0], [1.0, 2.0], [2.0, 0.0]]}}}}""")
        val bson = geoWithinPolygon(Person::address, listOf(listOf(0.0, 0.0), listOf(1.0, 2.0), listOf(2.0, 0.0)))
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::address.geoWithinPolygon(listOf(listOf(0.0, 0.0), listOf(1.0, 2.0), listOf(2.0, 0.0)))
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testGeoWithinCenterSupport() {
        val expected =
            BsonDocument.parse("""{"address": {"${'$'}geoWithin": {"${'$'}center":  [[1.0, 2.0], 30.0]}}}}""")
        val bson = geoWithinCenter(Person::address, 1.0, 2.0, 30.0)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::address.geoWithinCenter(1.0, 2.0, 30.0)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testGeoWithinCenterSphereSupport() {
        val expected =
            BsonDocument.parse("""{"address": {"${'$'}geoWithin": {"${'$'}centerSphere":  [[1.0, 2.0], 30.0]}}}}""")
        val bson = geoWithinCenterSphere(Person::address, 1.0, 2.0, 30.0)
        assertEquals(expected, bson.document)

        val kmongoDsl = Person::address.geoWithinCenterSphere(1.0, 2.0, 30.0)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testGeoIntersectsSupport() {
        val geometry =
            """{"${'$'}geometry":  {"type": "Polygon",
                | "coordinates": [[[1.0, 2.0], [2.0, 3.0], [3.0, 4.0], [1.0, 2.0]]]}}"""
                .trimMargin()
        val expected = BsonDocument.parse("""{"address": {"${'$'}geoIntersects": $geometry}}""")
        val polygon = Polygon(listOf(Position(1.0, 2.0), Position(2.0, 3.0), Position(3.0, 4.0), Position(1.0, 2.0)))
        var bson = geoIntersects(Person::address, polygon)
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::address.geoIntersects(polygon)
        assertEquals(expected, kmongoDsl.document)

        // Using Bson
        val bsonGeometry = BsonDocument.parse(geometry).getDocument("${'$'}geometry")
        bson = geoIntersects(Person::address, bsonGeometry)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.geoIntersects(bsonGeometry)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testNearSupport() {
        var geometry = """{"${'$'}geometry":  {"type": "Point", "coordinates": [1.0, 2.0]}}}"""
        var expected = BsonDocument.parse("""{"address": {"${'$'}near": $geometry}}""")
        val point = Point(Position(1.0, 2.0))
        var bson = near(Person::address, point)
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::address.near(point)
        assertEquals(expected, kmongoDsl.document)

        // Using Bson
        var bsonGeometry = BsonDocument.parse(geometry).getDocument("${'$'}geometry")
        bson = near(Person::address, bsonGeometry)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.near(bsonGeometry)
        assertEquals(expected, kmongoDsl.document)

        // Using short api
        expected = BsonDocument.parse("""{"address": {"${'$'}near": [1.0, 2.0]}}""")
        bson = near(Person::address, 1.0, 2.0)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.near(1.0, 2.0)
        assertEquals(expected, kmongoDsl.document)

        // With optionals
        geometry =
            """{"${'$'}geometry":  {"type": "Point", "coordinates": [1.0, 2.0]},
                |"${'$'}maxDistance": 10.0, "${'$'}minDistance": 1.0}"""
                .trimMargin()
        expected = BsonDocument.parse("""{"address": {"${'$'}near": $geometry}}""")
        bson = near(Person::address, point, 10.0, 1.0)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.near(point, 10.0, 1.0)
        assertEquals(expected, kmongoDsl.document)

        // Using Bson
        bsonGeometry = BsonDocument.parse(geometry).getDocument("${'$'}geometry")
        bson = near(Person::address, bsonGeometry, 10.0, 1.0)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.near(bsonGeometry, 10.0, 1.0)
        assertEquals(expected, kmongoDsl.document)

        // Using short api
        expected =
            BsonDocument.parse(
                """{"address": {"${'$'}near": [1.0, 2.0], "${'$'}maxDistance": 10.0, "${'$'}minDistance": 1.0}}""")
        bson = near(Person::address, 1.0, 2.0, 10.0, 1.0)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.near(1.0, 2.0, 10.0, 1.0)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testNearSphereSupport() {
        var geometry = """{"${'$'}geometry":  {"type": "Point", "coordinates": [1.0, 2.0]}}}"""
        var expected = BsonDocument.parse("""{"address": {"${'$'}nearSphere": $geometry}}""")
        val point = Point(Position(1.0, 2.0))
        var bson = nearSphere(Person::address, point)
        assertEquals(expected, bson.document)

        var kmongoDsl = Person::address.nearSphere(point)
        assertEquals(expected, kmongoDsl.document)

        // Using Bson
        var bsonGeometry = BsonDocument.parse(geometry).getDocument("${'$'}geometry")
        bson = nearSphere(Person::address, bsonGeometry)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.nearSphere(point)
        assertEquals(expected, kmongoDsl.document)

        // Using short api
        expected = BsonDocument.parse("""{"address": {"${'$'}nearSphere": [1.0, 2.0]}}""")
        bson = nearSphere(Person::address, 1.0, 2.0)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.nearSphere(1.0, 2.0)
        assertEquals(expected, kmongoDsl.document)

        // With optionals
        geometry =
            """{"${'$'}geometry":  {"type": "Point", "coordinates": [1.0, 2.0]},
                |"${'$'}maxDistance": 10.0, "${'$'}minDistance": 1.0}"""
                .trimMargin()
        expected = BsonDocument.parse("""{"address": {"${'$'}nearSphere": $geometry}}""")
        bson = nearSphere(Person::address, point, 10.0, 1.0)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.nearSphere(point, 10.0, 1.0)
        assertEquals(expected, kmongoDsl.document)

        // Using Bson
        bsonGeometry = BsonDocument.parse(geometry).getDocument("${'$'}geometry")
        bson = nearSphere(Person::address, bsonGeometry, 10.0, 1.0)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.nearSphere(point, 10.0, 1.0)
        assertEquals(expected, kmongoDsl.document)

        // Using short api
        expected =
            BsonDocument.parse(
                """{"address": {"${'$'}nearSphere": [1.0, 2.0],
                    |"${'$'}maxDistance": 10.0, "${'$'}minDistance": 1.0}}"""
                    .trimMargin())
        bson = nearSphere(Person::address, 1.0, 2.0, 10.0, 1.0)
        assertEquals(expected, bson.document)

        kmongoDsl = Person::address.nearSphere(1.0, 2.0, 10.0, 1.0)
        assertEquals(expected, kmongoDsl.document)
    }

    @Test
    fun testJsonSchemaSupport() {
        val expected = BsonDocument.parse("""{"${'$'}jsonSchema": {"bsonType": "object"}}""")

        val bson = jsonSchema(BsonDocument.parse("""{"bsonType": "object"}"""))
        assertEquals(expected, bson.document)
    }

    private val Bson.document: BsonDocument
        get() = toBsonDocument(BsonDocument::class.java, MongoClientSettings.getDefaultCodecRegistry())
}
