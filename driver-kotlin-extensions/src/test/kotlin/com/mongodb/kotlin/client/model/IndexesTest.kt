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

import com.mongodb.client.model.Indexes
import com.mongodb.client.model.Indexes.compoundIndex
import com.mongodb.kotlin.client.model.Indexes.ascending
import com.mongodb.kotlin.client.model.Indexes.descending
import com.mongodb.kotlin.client.model.Indexes.geo2d
import com.mongodb.kotlin.client.model.Indexes.geo2dsphere
import com.mongodb.kotlin.client.model.Indexes.hashed
import com.mongodb.kotlin.client.model.Indexes.text
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.conversions.Bson
import org.junit.Test

class IndexesTest {

    @Test
    fun `ascending index`() {
        assertEquals(""" {name: 1} """, ascending(Person::name))
        assertEquals(""" {name: 1, age: 1} """, ascending(Person::name, Person::age))
        assertEquals(""" {name: 1, age: 1} """, ascending(listOf(Person::name, Person::age)))
    }

    @Test
    fun `descending index`() {
        assertEquals(""" {name: -1} """, descending(Person::name))
        assertEquals(""" {name: -1, age: -1} """, descending(Person::name, Person::age))
        assertEquals(""" {name: -1, age: -1} """, descending(listOf(Person::name, Person::age)))
    }

    @Test
    fun `geo2dsphere index`() {
        assertEquals(""" {name: "2dsphere"} """, geo2dsphere(Person::name))
        assertEquals(""" {name: "2dsphere", age: "2dsphere"} """, geo2dsphere(Person::name, Person::age))
        assertEquals(""" {name: "2dsphere", age: "2dsphere"} """, geo2dsphere(listOf(Person::name, Person::age)))
    }

    @Test
    fun `geo2d index`() {
        assertEquals(""" {name: "2d"} """, geo2d(Person::name))
    }

    @Test
    fun `text helper`() {
        assertEquals(""" {name: "text"} """, text(Person::name))
        assertEquals(""" { "${'$'}**" : "text"} """, Indexes.text())
    }

    @Test
    fun `hashed index`() {
        assertEquals(""" {name: "hashed"} """, hashed(Person::name))
    }

    @Test
    fun `compound index`() {
        assertEquals(""" {name : 1, age : -1}  """, compoundIndex(ascending(Person::name), descending(Person::age)))
    }

    @Test
    fun `should test equals on CompoundIndex`() {
        assertEquals(
            compoundIndex(ascending(Person::name), descending(Person::age)),
            compoundIndex(ascending(Person::name), descending(Person::age)))

        assertEquals(
            compoundIndex(listOf(ascending(Person::name), descending(Person::age))),
            compoundIndex(listOf(ascending(Person::name), descending(Person::age))))
    }

    // Utils
    private data class Person(val name: String, val age: Int)

    private fun assertEquals(expected: String, result: Bson) =
        assertEquals(BsonDocument.parse(expected), result.toBsonDocument())
}
