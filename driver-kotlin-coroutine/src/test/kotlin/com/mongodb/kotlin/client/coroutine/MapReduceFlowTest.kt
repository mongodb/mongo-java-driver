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

package com.mongodb.kotlin.client.coroutine

import com.mongodb.client.model.Collation
import com.mongodb.client.model.MapReduceAction
import com.mongodb.reactivestreams.client.MapReducePublisher
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import reactor.core.publisher.Mono

class MapReduceFlowTest {
    @Test
    fun shouldHaveTheSameMethods() {
        val jMapReducePublisherFunctions = MapReducePublisher::class.declaredFunctions.map { it.name }.toSet() - "first"
        val kMapReduceFlowFunctions = MapReduceFlow::class.declaredFunctions.map { it.name }.toSet() - "collect"

        assertEquals(jMapReducePublisherFunctions, kMapReduceFlowFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: MapReducePublisher<Document> = mock()
        val flow = MapReduceFlow(wrapped)

        val batchSize = 10
        val bson = BsonDocument()
        val collation = Collation.builder().locale("en").build()
        val collectionName = "coll"
        val databaseName = "db"
        val filter = BsonDocument()
        val finalizeFunction = "finalize"

        flow.batchSize(batchSize)
        flow.bypassDocumentValidation(true)
        flow.collation(collation)
        flow.collectionName(collectionName)
        flow.databaseName(databaseName)
        flow.filter(filter)
        flow.finalizeFunction(finalizeFunction)
        flow.jsMode(true)
        flow.limit(1)
        flow.maxTime(1)
        flow.maxTime(1, TimeUnit.SECONDS)
        flow.nonAtomic(true)
        flow.scope(bson)
        flow.sharded(true)
        flow.sort(bson)
        flow.verbose(true)
        flow.action(MapReduceAction.MERGE)

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
        verify(wrapped).verbose(true)
        verify(wrapped).action(MapReduceAction.MERGE)

        whenever(wrapped.toCollection()).doReturn(Mono.empty())
        runBlocking { flow.toCollection() }
        verify(wrapped).toCollection()

        verifyNoMoreInteractions(wrapped)
    }
}
