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
package com.mongodb.kotlin.client.syncadapter

import com.mongodb.ExplainVerbosity
import com.mongodb.client.ListSearchIndexesIterable as JListSearchIndexesIterable
import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.client.model.Collation
import com.mongodb.kotlin.client.ListSearchIndexesIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.Document

internal class SyncListSearchIndexesIterable<T : Any>(val wrapped: ListSearchIndexesIterable<T>) :
    JListSearchIndexesIterable<T>, SyncMongoIterable<T>(wrapped) {
    override fun batchSize(batchSize: Int): SyncListSearchIndexesIterable<T> = apply { wrapped.batchSize(batchSize) }
    override fun timeoutMode(timeoutMode: TimeoutMode): SyncListSearchIndexesIterable<T> = apply {
        wrapped.timeoutMode(timeoutMode)
    }
    override fun name(indexName: String): SyncListSearchIndexesIterable<T> = apply { wrapped.name(indexName) }

    override fun allowDiskUse(allowDiskUse: Boolean?): com.mongodb.client.ListSearchIndexesIterable<T> = apply {
        wrapped.allowDiskUse(allowDiskUse)
    }

    @Suppress("DEPRECATION")
    @Deprecated("Prefer using the operation execution timeout configuration option", level = DeprecationLevel.HIDDEN)
    override fun maxTime(maxTime: Long, timeUnit: TimeUnit): SyncListSearchIndexesIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    override fun collation(collation: Collation?): com.mongodb.client.ListSearchIndexesIterable<T> = apply {
        wrapped.collation(collation)
    }

    override fun comment(comment: String?): SyncListSearchIndexesIterable<T> = apply { wrapped.comment(comment) }
    override fun comment(comment: BsonValue?): SyncListSearchIndexesIterable<T> = apply { wrapped.comment(comment) }
    override fun explain(): Document = wrapped.explain()

    override fun explain(verbosity: ExplainVerbosity): Document = wrapped.explain(verbosity)

    override fun <E : Any> explain(explainResultClass: Class<E>): E = wrapped.explain(explainResultClass)

    override fun <E : Any> explain(explainResultClass: Class<E>, verbosity: ExplainVerbosity): E =
        wrapped.explain(explainResultClass, verbosity)
}
