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
package com.mongodb.kotlin.client

import com.mongodb.ClientSessionOptions
import com.mongodb.TransactionOptions
import com.mongodb.client.ClientSession as JClientSession
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.functions
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever

class ClientSessionTest {

    @Test
    fun shouldHaveTheSameMethods() {
        val internalFunctions =
            setOf(
                "advanceClusterTime",
                "advanceOperationTime",
                "clearTransactionContext",
                "getClusterTime",
                "getOperationTime",
                "getOriginator",
                "getPinnedServerAddress",
                "getRecoveryToken",
                "getServerSession",
                "getSnapshotTimestamp",
                "getTransactionContext",
                "notifyMessageSent",
                "notifyOperationInitiated",
                "setRecoveryToken",
                "setSnapshotTimestamp",
                "setTransactionContext")

        val jClientSessionFunctions = JClientSession::class.functions.map { it.name }.toSet() - internalFunctions
        val kClientSessionFunctions =
            ClientSession::class.functions.map { it.name }.toSet() +
                ClientSession::class
                    .declaredMemberProperties
                    .filterNot { it.name == "wrapped" }
                    .map {
                        if (it.name.startsWith("is") || it.name.startsWith("has")) it.name
                        else "get${it.name.replaceFirstChar { c -> c.uppercaseChar()}}"
                    }

        assertEquals(jClientSessionFunctions, kClientSessionFunctions)
    }

    @Test
    fun shouldCallTheUnderlyingMethods() {
        val wrapped: JClientSession = mock()
        val session = ClientSession(wrapped)

        val transactionOptions = TransactionOptions.builder().maxCommitTime(10).build()

        whenever(wrapped.options).doReturn(ClientSessionOptions.builder().build())
        whenever(wrapped.isCausallyConsistent).doReturn(true)
        whenever(wrapped.transactionOptions).doReturn(transactionOptions)

        session.options
        session.isCausallyConsistent
        session.startTransaction()
        session.startTransaction(transactionOptions)
        session.transactionOptions

        verify(wrapped).options
        verify(wrapped).isCausallyConsistent
        verify(wrapped).startTransaction()
        verify(wrapped).startTransaction(transactionOptions)
        verify(wrapped).transactionOptions

        session.abortTransaction()
        session.commitTransaction()

        verify(wrapped).abortTransaction()
        verify(wrapped).commitTransaction()
        verifyNoMoreInteractions(wrapped)
    }
}
