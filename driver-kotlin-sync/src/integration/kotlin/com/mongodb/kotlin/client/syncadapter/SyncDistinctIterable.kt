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

import com.mongodb.client.DistinctIterable as JDistinctIterable
import com.mongodb.client.model.Collation
import com.mongodb.kotlin.client.DistinctIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.conversions.Bson

data class SyncDistinctIterable<T : Any>(val wrapped: DistinctIterable<T>) :
    JDistinctIterable<T>, SyncMongoIterable<T>(wrapped) {
    override fun batchSize(batchSize: Int): SyncDistinctIterable<T> = apply { wrapped.batchSize(batchSize) }
    override fun filter(filter: Bson?): SyncDistinctIterable<T> = apply { wrapped.filter(filter) }
    override fun maxTime(maxTime: Long, timeUnit: TimeUnit): SyncDistinctIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }
    override fun collation(collation: Collation?): SyncDistinctIterable<T> = apply { wrapped.collation(collation) }
    override fun comment(comment: String?): SyncDistinctIterable<T> = apply { wrapped.comment(comment) }
    override fun comment(comment: BsonValue?): SyncDistinctIterable<T> = apply { wrapped.comment(comment) }
}
