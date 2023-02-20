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
@file:Suppress("DEPRECATION")

package com.mongodb.kotlin.client

import com.mongodb.client.MapReduceIterable as JMapReduceIterable
import com.mongodb.client.model.Collation
import com.mongodb.client.model.MapReduceAction
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions

class MapReduceIterableTest {
    @Test
    fun shouldHaveTheSameMethods() {
        val jMapReduceIterableFunctions = JMapReduceIterable::class.declaredFunctions.map { it.name }.toSet()
        val kMapReduceIterableFunctions = MapReduceIterable::class.declaredFunctions.map { it.name }.toSet()

        assertEquals(jMapReduceIterableFunctions, kMapReduceIterableFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: JMapReduceIterable<Document> = mock()
        val iterable = MapReduceIterable(wrapped)

        val batchSize = 10
        val bson = BsonDocument()
        val collation = Collation.builder().locale("en").build()
        val collectionName = "coll"
        val databaseName = "db"
        val filter = BsonDocument()
        val finalizeFunction = "finalize"

        iterable.batchSize(batchSize)
        iterable.bypassDocumentValidation(true)
        iterable.collation(collation)
        iterable.collectionName(collectionName)
        iterable.databaseName(databaseName)
        iterable.filter(filter)
        iterable.finalizeFunction(finalizeFunction)
        iterable.jsMode(true)
        iterable.limit(1)
        iterable.maxTime(1)
        iterable.maxTime(1, TimeUnit.SECONDS)
        iterable.nonAtomic(true)
        iterable.scope(bson)
        iterable.sharded(true)
        iterable.sort(bson)
        iterable.toCollection()
        iterable.verbose(true)
        iterable.action(MapReduceAction.MERGE)

        verify(wrapped).batchSize(batchSize)
        verify(wrapped).bypassDocumentValidation(true)
        verify(wrapped).collation(collation)
        verify(wrapped).collectionName(collectionName)
        verify(wrapped).databaseName(databaseName)
        verify(wrapped).filter(filter)
        verify(wrapped).finalizeFunction(finalizeFunction)
        verify(wrapped).jsMode(true)
        verify(wrapped).limit(1)
        verify(wrapped).maxTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxTime(1, TimeUnit.SECONDS)
        verify(wrapped).nonAtomic(true)
        verify(wrapped).scope(bson)
        verify(wrapped).sharded(true)
        verify(wrapped).sort(bson)
        verify(wrapped).toCollection()
        verify(wrapped).verbose(true)
        verify(wrapped).action(MapReduceAction.MERGE)

        verifyNoMoreInteractions(wrapped)
    }
}
