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

import com.mongodb.reactivestreams.client.ListDatabasesPublisher
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredFunctions
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions

class ListDatabasesFlowTest {
    @Test
    fun shouldHaveTheSameMethods() {
        val jListDatabasesPublisherFunctions =
            ListDatabasesPublisher::class.declaredFunctions.map { it.name }.toSet() - "first"
        val kListDatabasesFlowFunctions = ListDatabasesFlow::class.declaredFunctions.map { it.name }.toSet() - "collect"

        assertEquals(jListDatabasesPublisherFunctions, kListDatabasesFlowFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: ListDatabasesPublisher<Document> = mock()
        val flow = ListDatabasesFlow(wrapped)

        val batchSize = 10
        val bsonComment = BsonString("a comment")
        val comment = "comment"
        val filter = BsonDocument()

        flow.authorizedDatabasesOnly(true)
        flow.batchSize(batchSize)
        flow.comment(bsonComment)
        flow.comment(comment)
        flow.filter(filter)
        flow.maxTime(1)
        flow.maxTime(1, TimeUnit.SECONDS)
        flow.nameOnly(true)

        verify(wrapped).authorizedDatabasesOnly(true)
        verify(wrapped).batchSize(batchSize)
        verify(wrapped).comment(bsonComment)
        verify(wrapped).comment(comment)
        verify(wrapped).filter(filter)
        verify(wrapped).maxTime(1, TimeUnit.MILLISECONDS)
        verify(wrapped).maxTime(1, TimeUnit.SECONDS)
        verify(wrapped).nameOnly(true)

        verifyNoMoreInteractions(wrapped)
    }
}
