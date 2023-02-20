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

import com.mongodb.CursorType
import com.mongodb.ExplainVerbosity
import com.mongodb.client.FindIterable as JFindIterable
import com.mongodb.client.model.Collation
import com.mongodb.kotlin.client.FindIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.Document
import org.bson.conversions.Bson

@Suppress("DEPRECATION")
data class SyncFindIterable<T>(val wrapped: FindIterable<T>) : JFindIterable<T>, SyncMongoIterable<T>(wrapped) {
    override fun batchSize(batchSize: Int): SyncFindIterable<T> = apply { wrapped.batchSize(batchSize) }
    override fun filter(filter: Bson?): SyncFindIterable<T> = apply { wrapped.filter(filter) }

    override fun limit(limit: Int): SyncFindIterable<T> = apply { wrapped.limit(limit) }

    override fun skip(skip: Int): SyncFindIterable<T> = apply { wrapped.skip(skip) }

    override fun allowDiskUse(allowDiskUse: Boolean?): SyncFindIterable<T> = apply {
        wrapped.allowDiskUse(allowDiskUse)
    }

    override fun maxTime(maxTime: Long, timeUnit: TimeUnit): SyncFindIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    override fun maxAwaitTime(maxAwaitTime: Long, timeUnit: TimeUnit): SyncFindIterable<T> = apply {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit)
    }

    override fun projection(projection: Bson?): SyncFindIterable<T> = apply { wrapped.projection(projection) }

    override fun sort(sort: Bson?): SyncFindIterable<T> = apply { wrapped.sort(sort) }

    override fun noCursorTimeout(noCursorTimeout: Boolean): SyncFindIterable<T> = apply {
        wrapped.noCursorTimeout(noCursorTimeout)
    }

    @Suppress("OVERRIDE_DEPRECATION")
    override fun oplogReplay(oplogReplay: Boolean): SyncFindIterable<T> = apply { wrapped.oplogReplay(oplogReplay) }

    override fun partial(partial: Boolean): SyncFindIterable<T> = apply { wrapped.partial(partial) }

    override fun cursorType(cursorType: CursorType): SyncFindIterable<T> = apply { wrapped.cursorType(cursorType) }

    override fun collation(collation: Collation?): SyncFindIterable<T> = apply { wrapped.collation(collation) }

    override fun comment(comment: String?): SyncFindIterable<T> = apply { wrapped.comment(comment) }

    override fun comment(comment: BsonValue?): SyncFindIterable<T> = apply { wrapped.comment(comment) }

    override fun hint(hint: Bson?): SyncFindIterable<T> = apply { wrapped.hint(hint) }

    override fun hintString(hint: String?): SyncFindIterable<T> = apply { wrapped.hintString(hint) }

    override fun let(variables: Bson?): SyncFindIterable<T> = apply { wrapped.let(variables) }
    override fun max(max: Bson?): SyncFindIterable<T> = apply { wrapped.max(max) }

    override fun min(min: Bson?): SyncFindIterable<T> = apply { wrapped.min(min) }

    override fun returnKey(returnKey: Boolean): SyncFindIterable<T> = apply { wrapped.returnKey(returnKey) }

    override fun showRecordId(showRecordId: Boolean): SyncFindIterable<T> = apply { wrapped.showRecordId(showRecordId) }

    override fun explain(): Document = wrapped.explain()

    override fun explain(verbosity: ExplainVerbosity): Document = wrapped.explain(verbosity)

    override fun <E : Any> explain(explainResultClass: Class<E>): E = wrapped.explain(explainResultClass)

    override fun <E : Any> explain(explainResultClass: Class<E>, verbosity: ExplainVerbosity): E =
        wrapped.explain(explainResultClass, verbosity)
}
