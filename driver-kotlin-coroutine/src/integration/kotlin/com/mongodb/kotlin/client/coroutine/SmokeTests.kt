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
package com.mongodb.kotlin.client.coroutine

import com.mongodb.client.Fixture.getDefaultDatabaseName
import com.mongodb.client.Fixture.getMongoClientSettings
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SmokeTests {

    @AfterEach
    fun afterEach() {
        runBlocking { database?.drop() }
    }

    @Test
    @DisplayName("distinct and return nulls")
    fun testDistinctNullable() = runTest {
        collection!!.insertMany(
            listOf(
                Document.parse("{_id: 1, a: 0}"),
                Document.parse("{_id: 2, a: 1}"),
                Document.parse("{_id: 3, a: 0}"),
                Document.parse("{_id: 4, a: null}")))

        // nulls are auto excluded in reactive streams!
        val actual = collection!!.distinct<Int>("a").toSet()
        assertEquals(setOf(0, 1), actual)
    }

    @Test
    @DisplayName("mapping can return nulls")
    fun testMongoIterableMap() = runTest {
        collection!!.insertMany(
            listOf(
                Document.parse("{_id: 1, a: 0}"),
                Document.parse("{_id: 2, a: 1}"),
                Document.parse("{_id: 3, a: 0}"),
                Document.parse("{_id: 4, a: null}")))

        val actual = collection!!.find().map { it["a"] as Int? }.toList()
        assertContentEquals(listOf(0, 1, 0, null), actual)
    }

    companion object {

        private var mongoClient: MongoClient? = null
        private var database: MongoDatabase? = null
        private var collection: MongoCollection<Document>? = null

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            runBlocking {
                mongoClient = MongoClient.create(getMongoClientSettings())
                database = mongoClient?.getDatabase(getDefaultDatabaseName())
                database?.drop()
                collection = database?.getCollection("SmokeTests")
            }
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            runBlocking {
                collection = null
                database?.drop()
                database = null
                mongoClient?.close()
                mongoClient = null
            }
        }
    }
}
