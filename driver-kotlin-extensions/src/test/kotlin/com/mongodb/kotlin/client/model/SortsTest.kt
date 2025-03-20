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

import com.mongodb.client.model.Sorts.orderBy
import com.mongodb.kotlin.client.model.Sorts.ascending
import com.mongodb.kotlin.client.model.Sorts.descending
import com.mongodb.kotlin.client.model.Sorts.metaTextScore
import kotlin.test.Test
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.conversions.Bson

class SortsTest {

    @Test
    fun ascending() {
        assertEquals(""" {name : 1}  """, ascending(Person::name))
        assertEquals(""" {name : 1, age: 1}  """, ascending(Person::name, Person::age))
        assertEquals(""" {name : 1, age: 1}  """, ascending(listOf(Person::name, Person::age)))
    }

    @Test
    fun descending() {
        assertEquals(""" {name : -1}  """, descending(Person::name))
        assertEquals(""" {name : -1, age: -1}  """, descending(Person::name, Person::age))
        assertEquals(""" {name : -1, age: -1}  """, descending(listOf(Person::name, Person::age)))
    }

    @Test
    fun metaTextScore() {
        assertEquals(""" {name : {${'$'}meta : "textScore"}}  """, metaTextScore(Person::name))
    }

    @Test
    fun orderBy() {
        assertEquals(""" {name : 1, age : -1}  """, orderBy(ascending(Person::name), descending(Person::age)))
        assertEquals(""" {name : 1, age : -1}  """, orderBy(listOf(ascending(Person::name), descending(Person::age))))
        assertEquals(
            """ {name : -1, age : -1}  """,
            orderBy(ascending(Person::name), descending(Person::age), descending(Person::name)))
        assertEquals(
            """ {name : 1, age : 1, results: -1, address: -1}  """,
            orderBy(ascending(Person::name, Person::age), descending(Person::results, Person::address)))
    }

    @Test
    fun `should create string representation for compound sorts`() {
        assertEquals(
            """Compound Sort{sorts=[{"name": 1, "age": 1}, {"results": -1, "address": -1}]}""",
            orderBy(ascending(Person::name, Person::age), descending(Person::results, Person::address)).toString())
    }

    private data class Person(val name: String, val age: Int, val address: List<String>, val results: List<Int>)
    private fun assertEquals(expected: String, result: Bson) =
        assertEquals(BsonDocument.parse(expected), result.toBsonDocument())
}
