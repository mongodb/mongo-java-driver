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

package com.mongodb.kotlin.client.coroutine.syncadapter

import com.mongodb.client.MapReduceIterable as JMapReduceIterable
import com.mongodb.client.model.Collation
import com.mongodb.client.model.MapReduceAction
import com.mongodb.kotlin.client.coroutine.MapReduceFlow
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import org.bson.conversions.Bson

data class SyncMapReduceIterable<T : Any>(val wrapped: MapReduceFlow<T>) :
    JMapReduceIterable<T>, SyncMongoIterable<T>(wrapped) {
    override fun batchSize(batchSize: Int): SyncMapReduceIterable<T> = apply { wrapped.batchSize(batchSize) }
    override fun toCollection() = runBlocking { wrapped.toCollection() }
    override fun collectionName(collectionName: String): SyncMapReduceIterable<T> = apply {
        wrapped.collectionName(collectionName)
    }

    override fun finalizeFunction(finalizeFunction: String?): SyncMapReduceIterable<T> = apply {
        wrapped.finalizeFunction(finalizeFunction)
    }

    override fun scope(scope: Bson?): SyncMapReduceIterable<T> = apply { wrapped.scope(scope) }
    override fun sort(sort: Bson?): SyncMapReduceIterable<T> = apply { wrapped.sort(sort) }
    override fun filter(filter: Bson?): SyncMapReduceIterable<T> = apply { wrapped.filter(filter) }
    override fun limit(limit: Int): SyncMapReduceIterable<T> = apply { wrapped.limit(limit) }
    override fun jsMode(jsMode: Boolean): SyncMapReduceIterable<T> = apply { wrapped.jsMode(jsMode) }
    override fun verbose(verbose: Boolean): SyncMapReduceIterable<T> = apply { wrapped.verbose(verbose) }

    override fun maxTime(maxTime: Long, timeUnit: TimeUnit): SyncMapReduceIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }
    override fun action(action: MapReduceAction): SyncMapReduceIterable<T> = apply { wrapped.action(action) }
    override fun databaseName(databaseName: String?): SyncMapReduceIterable<T> = apply {
        wrapped.databaseName(databaseName)
    }
    @Suppress("OVERRIDE_DEPRECATION")
    override fun sharded(sharded: Boolean): SyncMapReduceIterable<T> = apply { wrapped.sharded(sharded) }
    @Suppress("OVERRIDE_DEPRECATION")
    override fun nonAtomic(nonAtomic: Boolean): SyncMapReduceIterable<T> = apply { wrapped.nonAtomic(nonAtomic) }

    override fun bypassDocumentValidation(bypassDocumentValidation: Boolean?): SyncMapReduceIterable<T> = apply {
        wrapped.bypassDocumentValidation(bypassDocumentValidation)
    }

    override fun collation(collation: Collation?): SyncMapReduceIterable<T> = apply { wrapped.collation(collation) }
}
