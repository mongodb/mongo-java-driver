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

import com.mongodb.client.model.Aggregates.project
import com.mongodb.client.model.Projections.excludeId
import com.mongodb.client.model.Projections.fields
import com.mongodb.kotlin.client.model.Filters.and
import com.mongodb.kotlin.client.model.Filters.eq
import com.mongodb.kotlin.client.model.Filters.gt
import com.mongodb.kotlin.client.model.Projections.computed
import com.mongodb.kotlin.client.model.Projections.computedSearchMeta
import com.mongodb.kotlin.client.model.Projections.elemMatch
import com.mongodb.kotlin.client.model.Projections.exclude
import com.mongodb.kotlin.client.model.Projections.include
import com.mongodb.kotlin.client.model.Projections.meta
import com.mongodb.kotlin.client.model.Projections.metaSearchHighlights
import com.mongodb.kotlin.client.model.Projections.metaSearchScore
import com.mongodb.kotlin.client.model.Projections.metaTextScore
import com.mongodb.kotlin.client.model.Projections.metaVectorSearchScore
import com.mongodb.kotlin.client.model.Projections.projection
import com.mongodb.kotlin.client.model.Projections.projectionWith
import com.mongodb.kotlin.client.model.Projections.slice
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.conversions.Bson
import org.junit.Test

class ProjectionTest {

    @Test
    fun projection() {
        assertEquals("\$name", Person::name.projection)
        assertEquals("\$name.foo", Student::name.projectionWith("foo"))
    }

    @Test
    fun include() {
        assertEquals(
            """{"name": 1, "age": 1, "results": 1, "address": 1}""",
            include(Person::name, Person::age, Person::results, Person::address))

        assertEquals("""{"name": 1, "age": 1}""", include(listOf(Person::name, Person::age)))
        assertEquals("""{"name": 1, "age": 1}""", include(listOf(Person::name, Person::age, Person::name)))
    }

    @Test
    fun exclude() {
        assertEquals(
            """{"name": 0, "age": 0, "results": 0, "address": 0}""",
            exclude(Person::name, Person::age, Person::results, Person::address))
        assertEquals("""{"name": 0, "age": 0}""", exclude(listOf(Person::name, Person::age)))
        assertEquals("""{"name": 0, "age": 0}""", exclude(listOf(Person::name, Person::age, Person::name)))

        assertEquals("""{"name": 0, "age": 0}""", exclude(listOf(Person::name, Person::age, Person::name)))

        assertEquals("""{"_id": 0}""", excludeId())
        assertEquals(
            "Projections{projections=[{\"_id\": 0}, {\"name\": 1}]}",
            fields(excludeId(), include(Person::name)).toString())
    }

    @Test
    fun firstElem() {
        assertEquals(""" {"name.${'$'}" : 1} """, Person::name.elemMatch)
    }

    @Test
    fun elemMatch() {
        val expected =
            """
            {"grades": {"${'$'}elemMatch": {"${'$'}and": [{"subject": "Math"}, {"score": {"${'$'}gt": 80}}]}}}
        """
        assertEquals(expected, Student::grades.elemMatch(and((Grade::subject eq "Math"), (Grade::score gt 80))))

        assertEquals(Student::grades elemMatch (Grade::score gt 80), elemMatch(Student::grades, Grade::score gt 80))

        // Should create string representation for elemMatch with filter
        assertEquals(
            "ElemMatch Projection{fieldName='grades'," +
                " filter=And Filter{filters=[Filter{fieldName='score', value=90}, " +
                "Filter{fieldName='subject', value=Math}]}}",
            Student::grades.elemMatch(and(Grade::score eq 90, Grade::subject eq "Math")).toString())
    }

    @Test
    fun slice() {
        var expected = """
            {"grades": {"${'$'}slice": -1}}
        """

        assertEquals(expected, Student::grades.slice(-1))

        // skip one, limit to two
        expected = """
            {"grades": {"${'$'}slice": [1, 2]}}
        """

        assertEquals(expected, Student::grades.slice(1, 2))

        // Combining projection
        expected = """
           {"name": 0, "grades": {"${'$'}slice": [2, 1]}}
        """

        assertEquals(expected, fields(exclude(Student::name), Student::grades.slice(2, 1)))
    }

    @Test
    fun meta() {
        var expected = """
            {"score": {"${'$'}meta": "textScore"}}
        """
        assertEquals(expected, Grade::score.metaTextScore())

        // combining
        expected =
            """
            {"_id": 0, "score": {"${'$'}meta": "textScore"}, "grades": {"${'$'}elemMatch": {"score": {"${'$'}gt": 87}}}}
        """
        assertEquals(
            expected, fields(excludeId(), Grade::score.metaTextScore(), Student::grades.elemMatch(Grade::score gt 87)))

        expected = """
            {"score": {"${'$'}meta": "searchScore"}}
        """
        assertEquals(expected, Grade::score.metaSearchScore())

        expected = """
            {"score": {"${'$'}meta": "searchHighlights"}}
        """
        assertEquals(expected, Grade::score.metaSearchHighlights())

        expected = """
            {"score": {"${'$'}meta": "vectorSearchScore"}}
        """
        assertEquals(expected, Grade::score.metaVectorSearchScore())
        assertEquals(expected, Grade::score.meta("vectorSearchScore"))

        expected = """
            {"_id": 0, "score": {"${'$'}meta": "vectorSearchScore"}}
        """
        assertEquals(expected, fields(excludeId(), Grade::score meta "vectorSearchScore"))

        assertEquals(Grade::score meta "vectorSearchScore", meta(Grade::score, "vectorSearchScore"))
    }

    @Test
    fun `computed projection`() {
        assertEquals(""" {"c": "${'$'}y"} """, "c" computed "\$y")

        assertEquals(computed(Grade::score, Student::age), Grade::score computed Student::age)

        assertEquals(
            """{"${'$'}project": {"c": "${'$'}name", "score": "${'$'}age"}}""",
            project(fields("c" computed Student::name, Grade::score computed Student::age)))

        assertEquals(
            fields("c" computed Student::name, Grade::score computed Student::age),
            fields(computed("c", Student::name), Grade::score computed Student::age))

        assertEquals(
            """{"${'$'}project": {"c": "${'$'}name", "score": "${'$'}age"}}""",
            project(fields("c" computed Student::name, Grade::score computed Student::age)))

        assertEquals(
            fields(listOf("c" computed Student::name, Grade::score computed Student::age)),
            fields("c" computed Student::name, Grade::score computed Student::age))

        // combine fields
        assertEquals("{name : 1, age : 1, _id : 0}", fields(include(Student::name, Student::age), excludeId()))

        assertEquals("{name : 1, age : 1, _id : 0}", fields(include(listOf(Student::name, Student::age)), excludeId()))

        assertEquals("{name : 1, age : 0}", fields(include(Student::name, Student::age), exclude(Student::age)))

        // computedSearchMeta
        assertEquals("""{"name": "${'$'}${'$'}SEARCH_META"}""", Student::name.computedSearchMeta())
        assertEquals(Student::name.computedSearchMeta(), computedSearchMeta(Student::name))

        // Should create string representation for include and exclude
        assertEquals("""{"age": 1, "name": 1}""", include(Student::name, Student::age, Student::name).toString())
        assertEquals("""{"age": 0, "name": 0}""", exclude(Student::name, Student::age, Student::name).toString())
        assertEquals("""{"_id": 0}""", excludeId().toString())

        // Should create string representation for computed
        assertEquals(
            "Expression{name='c', expression=\$y}",
            com.mongodb.client.model.Projections.computed("c", "\$y").toString())
        assertEquals("Expression{name='c', expression=\$y}", ("c" computed "\$y").toString())
        assertEquals("Expression{name='name', expression=\$y}", (Student::name computed "\$y").toString())
    }

    @Test
    fun `array projection`() {
        assertEquals("{ \"grades.${'$'}\": 1 }", include(Student::grades.posOp))
        assertEquals("{ \"grades.comments.${'$'}\": 1 }", include((Student::grades / Grade::comments).posOp))
    }

    @Test
    fun `projection in aggregation`() {
        // Field Reference in Aggregation
        assertEquals(
            """ {"${'$'}project": {"score": "${'$'}grades.score"}} """,
            project((Grade::score computed (Student::grades.projectionWith("score")))))

        assertEquals(
            "{\"${'$'}project\": {\"My Score\": \"${'$'}grades.score\"}}",
            project("My Score" computed (Student::grades / Grade::score)))

        assertEquals("{\"${'$'}project\": {\"My Age\": \"${'$'}age\"}}", project("My Age" computed Student::age))

        assertEquals(
            "{\"${'$'}project\": {\"My Age\": \"${'$'}age\"}}", project("My Age" computed Student::age.projection))
    }

    private data class Person(val name: String, val age: Int, val address: List<String>, val results: List<Int>)
    private data class Student(val name: String, val age: Int, val grades: List<Grade>)
    private data class Grade(val subject: String, val score: Int, val comments: List<String>)

    private fun assertEquals(expected: String, result: Bson) =
        assertEquals(BsonDocument.parse(expected), result.toBsonDocument())
}
