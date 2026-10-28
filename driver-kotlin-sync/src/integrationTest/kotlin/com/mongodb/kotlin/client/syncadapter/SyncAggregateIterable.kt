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
import com.mongodb.client.AggregateIterable as JAggregateIterable
import com.mongodb.client.model.Collation
import com.mongodb.kotlin.client.AggregateIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.Document
import org.bson.conversions.Bson

internal class SyncAggregateIterable<T : Any>(val wrapped: AggregateIterable<T>) :
    JAggregateIterable<T>, SyncMongoIterable<T>(wrapped) {
    override fun batchSize(batchSize: Int): SyncAggregateIterable<T> = apply { wrapped.batchSize(batchSize) }

    override fun toCollection() = wrapped.toCollection()

    override fun allowDiskUse(allowDiskUse: Boolean?): SyncAggregateIterable<T> = apply {
        wrapped.allowDiskUse(allowDiskUse)
    }

    override fun maxTime(maxTime: Long, timeUnit: TimeUnit): SyncAggregateIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    override fun maxAwaitTime(maxAwaitTime: Long, timeUnit: TimeUnit): SyncAggregateIterable<T> = apply {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit)
    }

    override fun bypassDocumentValidation(bypassDocumentValidation: Boolean?): SyncAggregateIterable<T> = apply {
        wrapped.bypassDocumentValidation(bypassDocumentValidation)
    }

    override fun collation(collation: Collation?): SyncAggregateIterable<T> = apply { wrapped.collation(collation) }

    override fun comment(comment: String?): SyncAggregateIterable<T> = apply { wrapped.comment(comment) }

    override fun comment(comment: BsonValue?): SyncAggregateIterable<T> = apply { wrapped.comment(comment) }

    override fun hint(hint: Bson?): SyncAggregateIterable<T> = apply { wrapped.hint(hint) }

    override fun hintString(hint: String?): SyncAggregateIterable<T> = apply { wrapped.hintString(hint) }

    override fun let(variables: Bson?): SyncAggregateIterable<T> = apply { wrapped.let(variables) }

    override fun explain(): Document = wrapped.explain()

    override fun explain(verbosity: ExplainVerbosity): Document = wrapped.explain(verbosity)

    override fun <E : Any> explain(explainResultClass: Class<E>): E = wrapped.explain(explainResultClass)

    override fun <E : Any> explain(explainResultClass: Class<E>, verbosity: ExplainVerbosity): E =
        wrapped.explain(explainResultClass, verbosity)
}
