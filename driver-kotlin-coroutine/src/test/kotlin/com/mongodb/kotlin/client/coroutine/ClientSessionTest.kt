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

import com.mongodb.ClientSessionOptions
import com.mongodb.TransactionOptions
import com.mongodb.reactivestreams.client.ClientSession as JClientSession
import kotlin.reflect.full.functions
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import reactor.core.publisher.Mono

class ClientSessionTest {

    @Test
    fun shouldHaveTheSameMethods() {
        val jClientSessionFunctions = JClientSession::class.functions.map { it.name }.toSet()
        val kClientSessionFunctions = ClientSession::class.functions.map { it.name }.toSet()

        assertEquals(jClientSessionFunctions, kClientSessionFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: JClientSession = mock()
        val session = ClientSession(wrapped)

        val transactionOptions = TransactionOptions.builder().maxCommitTime(10).build()

        whenever(wrapped.options).doReturn(ClientSessionOptions.builder().build())
        whenever(wrapped.serverSession).doReturn(mock())
        whenever(wrapped.isCausallyConsistent).doReturn(true)
        whenever(wrapped.transactionOptions).doReturn(transactionOptions)

        session.options
        session.serverSession
        session.isCausallyConsistent
        session.startTransaction()
        session.startTransaction(transactionOptions)
        session.getTransactionOptions()

        verify(wrapped).options
        verify(wrapped).serverSession
        verify(wrapped).isCausallyConsistent
        verify(wrapped).startTransaction()
        verify(wrapped).startTransaction(transactionOptions)
        verify(wrapped).transactionOptions

        whenever(wrapped.abortTransaction()).doReturn(Mono.empty())
        whenever(wrapped.commitTransaction()).doReturn(Mono.empty())

        runBlocking {
            session.abortTransaction()
            session.commitTransaction()
        }

        verify(wrapped).abortTransaction()
        verify(wrapped).commitTransaction()
        verifyNoMoreInteractions(wrapped)
    }
}
