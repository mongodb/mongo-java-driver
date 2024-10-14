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

import com.mongodb.client.model.PushOptions
import com.mongodb.kotlin.client.model.Filters.gte
import com.mongodb.kotlin.client.model.Updates.addEachToSet
import com.mongodb.kotlin.client.model.Updates.addToSet
import com.mongodb.kotlin.client.model.Updates.bitwiseAnd
import com.mongodb.kotlin.client.model.Updates.bitwiseOr
import com.mongodb.kotlin.client.model.Updates.bitwiseXor
import com.mongodb.kotlin.client.model.Updates.combine
import com.mongodb.kotlin.client.model.Updates.currentDate
import com.mongodb.kotlin.client.model.Updates.currentTimestamp
import com.mongodb.kotlin.client.model.Updates.inc
import com.mongodb.kotlin.client.model.Updates.max
import com.mongodb.kotlin.client.model.Updates.min
import com.mongodb.kotlin.client.model.Updates.mul
import com.mongodb.kotlin.client.model.Updates.popFirst
import com.mongodb.kotlin.client.model.Updates.popLast
import com.mongodb.kotlin.client.model.Updates.pull
import com.mongodb.kotlin.client.model.Updates.pullAll
import com.mongodb.kotlin.client.model.Updates.pullByFilter
import com.mongodb.kotlin.client.model.Updates.push
import com.mongodb.kotlin.client.model.Updates.pushEach
import com.mongodb.kotlin.client.model.Updates.rename
import com.mongodb.kotlin.client.model.Updates.set
import com.mongodb.kotlin.client.model.Updates.setOnInsert
import com.mongodb.kotlin.client.model.Updates.unset
import java.time.Instant
import java.util.Date
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.pojo.PojoCodecProvider
import org.bson.conversions.Bson
import org.junit.Test

class UpdatesTest {
    @Test
    fun `should render $set`() {
        assertEquals(""" {"${'$'}set": {"name": "foo"}} """, set(Person::name, "foo"))
        assertEquals(""" {"${'$'}set": {"name": null}} """, set(Person::name, null))

        assertEquals(set(Person::name, "foo"), Person::name set "foo")
        assertEquals(set(Person::name, null), Person::name set null)
    }

    @Test
    fun `should render $setOnInsert`() {
        assertEquals(""" {${'$'}setOnInsert : { age : 42} } """, setOnInsert(Person::age, 42))
        assertEquals(setOnInsert(Person::age, 42), Person::age setOnInsert 42)

        assertEquals(""" {${'$'}setOnInsert : { age : null} } """, setOnInsert(Person::age, null))
        assertEquals(setOnInsert(Person::age, null), Person::age setOnInsert null)

        assertEquals(
            """ {"${'$'}setOnInsert": {"name": "foo", "age": 42}}""",
            combine(listOf(setOnInsert(Person::name, "foo"), setOnInsert(Person::age, 42))))
    }

    @Test
    fun `should render $unset`() {
        assertEquals(""" {"${'$'}unset": {"name": ""}} """, unset(Person::name))
    }

    @Test
    fun `should render $rename`() {
        assertEquals(""" {${'$'}rename : { "age" : "score"} } """, rename(Person::age, Grade::score))
    }

    @Test
    fun `should render $inc`() {
        assertEquals(""" {${'$'}inc : { age : 1} } """, inc(Person::age, 1))
        assertEquals(inc(Person::age, 1), Person::age inc 1)

        assertEquals(""" {${'$'}inc : { age : {${'$'}numberLong : "42"}} } """, inc(Person::age, 42L))
        assertEquals(""" {${'$'}inc : { age :  3.14 } } """, inc(Person::age, 3.14))
    }

    @Test
    fun `should render $mul`() {
        assertEquals(""" {${'$'}mul : { "age" : 1} }  """, mul(Person::age, 1))
        assertEquals(""" {${'$'}mul : { "age" : 1} }  """, Person::age mul 1)

        assertEquals(""" {${'$'}mul : { "age" : {${'$'}numberLong : "5"}} }  """, mul(Person::age, 5L))
        assertEquals(""" {${'$'}mul : { "age" : 3.14} }  """, mul(Person::age, 3.14))
    }

    @Test
    fun `should render $min`() {
        assertEquals(""" {${'$'}min : { age : 42} }  """, min(Person::age, 42))
        assertEquals(""" {${'$'}min : { age : 42} }  """, Person::age min 42)
    }

    @Test
    fun `should render max`() {
        assertEquals(""" {${'$'}max : { age : 42} }  """, max(Person::age, 42))
        assertEquals(""" {${'$'}max : { age : 42} }  """, Person::age max 42)
    }

    @Test
    fun `should render $currentDate`() {
        assertEquals("""  {${'$'}currentDate : { date : true} } """, currentDate(Person::date))
        assertEquals(
            """  {${'$'}currentDate : { date : {${'$'}type : "timestamp"}} } """, currentTimestamp(Person::date))
    }

    @Test
    fun `should render $addToSet`() {
        assertEquals(""" {${'$'}addToSet : { results : 1} } """, addToSet(Person::results, 1))
        assertEquals(""" {${'$'}addToSet : { results : 1} } """, Person::results addToSet 1)
        assertEquals(
            """ {"${'$'}addToSet": {"results": {"${'$'}each": [1, 2, 3]}}} """,
            addEachToSet(Person::results, listOf(1, 2, 3)))
    }

    @Test
    fun `should render $push`() {
        assertEquals(""" {${'$'}push : { results : 1} } """, push(Person::results, 1))
        assertEquals(
            """ {"${'$'}push": {"results": {"${'$'}each": [1, 2, 3]}}} """,
            pushEach(Person::results, listOf(1, 2, 3), options = PushOptions()))

        assertEquals(
            """ {"${'$'}push": {"grades": {"${'$'}each":
                |[{"comments": [], "score": 11, "subject": "Science"}],
                | "${'$'}position": 0, "${'$'}slice": 3, "${'$'}sort": {"score": -1}}}} """
                .trimMargin(),
            pushEach(
                Student::grades,
                listOf(Grade("Science", 11, emptyList())),
                options = PushOptions().position(0).slice(3).sortDocument(Document("score", -1))))

        assertEquals(
            """ {${'$'}push : { results : { ${'$'}each :
                |[89, 65], ${'$'}position : 0, ${'$'}slice : 3, ${'$'}sort : -1 } } } """
                .trimMargin(),
            pushEach(Person::results, listOf(89, 65), options = PushOptions().position(0).slice(3).sort(-1)))
    }

    @Test
    fun `should render $pull`() {
        assertEquals(""" {${'$'}pull : { address : "foo"} } """, pull(Person::address, "foo"))
        assertEquals(""" {${'$'}pull : { address : "foo"} } """, Person::address pull "foo")

        assertEquals(""" {${'$'}pull : { score : { ${'$'}gte : 5 }} } """, pullByFilter(Grade::score gte 5))
        assertEquals(
            """ {"${'$'}pull": {"grades": {"score": {"${'$'}gte": 5}}}} """,
            pullByFilter(Student::grades, Grade::score gte 5))
        assertEquals(
            """ {"${'$'}pull": {"grades": {"score": {"${'$'}gte": 5}}}} """,
            Student::grades pullByFilter (Grade::score gte 5))
    }

    @Test
    fun `should render $pullAll`() {
        assertEquals(""" {${'$'}pullAll : { results : []} }  """, pullAll(Person::results, emptyList()))
        assertEquals(""" {${'$'}pullAll : { results : []} }  """, Person::results pullAll emptyList())
        assertEquals(""" {${'$'}pullAll : { results : [1,2,3]} }  """, pullAll(Person::results, listOf(1, 2, 3)))
    }

    @Test
    fun `should render $pop`() {
        assertEquals(""" {${'$'}pop : { address : -1} } """, popFirst(Person::address))
        assertEquals(""" {${'$'}pop : { address : 1} } """, popLast(Person::address))
    }

    @Test
    fun `should render $bit`() {
        assertEquals("""  {${'$'}bit : { "score" : {and : 5} } } """, bitwiseAnd(Grade::score, 5))
        assertEquals("""  {${'$'}bit : { "score" : {and : 5} } } """, Grade::score bitwiseAnd 5)
        assertEquals(
            """  {${'$'}bit : { "score" : {and : {${'$'}numberLong : "5"}} } } """, bitwiseAnd(Grade::score, 5L))
        assertEquals(
            """  {${'$'}bit : { "score" : {and : {${'$'}numberLong : "5"}} } } """, Grade::score bitwiseAnd (5L))
        assertEquals("""  {${'$'}bit : { "score" : {or : 5} } } """, bitwiseOr(Grade::score, 5))
        assertEquals("""  {${'$'}bit : { "score" : {or : 5} } } """, Grade::score bitwiseOr 5)
        assertEquals("""  {${'$'}bit : { "score" : {or : {${'$'}numberLong : "5"}} } } """, bitwiseOr(Grade::score, 5L))
        assertEquals("""  {${'$'}bit : { "score" : {or : {${'$'}numberLong : "5"}} } } """, Grade::score bitwiseOr 5L)
        assertEquals("""  {${'$'}bit : { "score" : {xor : 5} } } """, bitwiseXor(Grade::score, 5))
        assertEquals("""  {${'$'}bit : { "score" : {xor : 5} } } """, Grade::score bitwiseXor 5)
        assertEquals("""  {${'$'}bit : { "score" : {xor : {${'$'}numberLong : "5"}} } } """, Grade::score bitwiseXor 5L)
        assertEquals(
            """  {${'$'}bit : { "score" : {xor : {${'$'}numberLong : "5"}} } } """, bitwiseXor(Grade::score, 5L))
    }

    @Test
    fun `should combine updates`() {
        assertEquals(""" {${'$'}set : { name : "foo"} } """, combine(set(Person::name, "foo")))
        assertEquals(
            """ {${'$'}set : { name : "foo", age: 42} } """, combine(set(Person::name, "foo"), set(Person::age, 42)))
        assertEquals(
            """ {${'$'}set : { name : "bar"} } """, combine(set(Person::name, "foo"), set(Person::name, "bar")))
        assertEquals(
            """ {"${'$'}set": {"name": "foo", "date": {"${'$'}date": "1970-01-01T00:00:00Z"}},
                | "${'$'}inc": {"age": 3, "floatField": 3.14}} """
                .trimMargin(),
            combine(
                set(Person::name, "foo"),
                inc(Person::age, 3),
                set(Person::date, Date.from(Instant.EPOCH)),
                inc(Person::floatField, 3.14)))

        assertEquals(""" {${'$'}set : { "name" : "foo"} } """, combine(combine(set(Person::name, "foo"))))
        assertEquals(
            """ {${'$'}set : { "name" : "foo", "age": 42} } """,
            combine(combine(set(Person::name, "foo"), set(Person::age, 42))))
        assertEquals(
            """ {${'$'}set : { "name" : "bar"} } """,
            combine(combine(set(Person::name, "foo"), set(Person::name, "bar"))))

        assertEquals(
            """ {"${'$'}set": {"name": "bar"}, "${'$'}inc": {"age": 3, "floatField": 3.14}}  """,
            combine(
                combine(
                    set(Person::name, "foo"),
                    inc(Person::age, 3),
                    set(Person::name, "bar"),
                    inc(Person::floatField, 3.14))))
    }

    @Test
    fun `should create string representation for simple updates`() {
        assertEquals(
            """Update{fieldName='name', operator='${'$'}set', value=foo}""", set(Person::name, "foo").toString())
    }

    @Test
    fun `should create string representation for with each update`() {
        assertEquals(
            """Each Update{fieldName='results', operator='${'$'}addToSet', values=[1, 2, 3]}""",
            addEachToSet(Person::results, listOf(1, 2, 3)).toString())
    }

    @Test
    fun `should test equals for SimpleBsonKeyValue`() {
        assertEquals(setOnInsert(Person::name, "foo"), setOnInsert(Person::name, "foo"))
        assertEquals(setOnInsert(Person::name, null), setOnInsert(Person::name, null))
    }

    @Test
    fun `should test hashCode for SimpleBsonKeyValue`() {
        assertEquals(setOnInsert(Person::name, "foo").hashCode(), setOnInsert(Person::name, "foo").hashCode())
        assertEquals(setOnInsert(Person::name, null).hashCode(), setOnInsert(Person::name, null).hashCode())
    }

    // Utils
    private data class Person(
        val name: String,
        val age: Int,
        val address: List<String>,
        val results: List<Int>,
        val date: Date,
        val floatField: Float
    )

    private data class Student(val name: String, val grade: Int, val grades: List<Grade>)
    data class Grade(val subject: String, val score: Int?, val comments: List<String>)

    private val defaultsAndPojoCodecRegistry: CodecRegistry =
        CodecRegistries.fromRegistries(
            Bson.DEFAULT_CODEC_REGISTRY, fromProviders(PojoCodecProvider.builder().automatic(true).build()))

    private fun assertEquals(expected: String, result: Bson) =
        assertEquals(
            BsonDocument.parse(expected), result.toBsonDocument(BsonDocument::class.java, defaultsAndPojoCodecRegistry))
}
