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

import com.mongodb.client.ListCollectionsIterable as JListCollectionsIterable
import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.kotlin.client.ListCollectionsIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.conversions.Bson

internal class SyncListCollectionsIterable<T : Any>(val wrapped: ListCollectionsIterable<T>) :
    JListCollectionsIterable<T>, SyncMongoIterable<T>(wrapped) {

    override fun batchSize(batchSize: Int): SyncListCollectionsIterable<T> = apply { wrapped.batchSize(batchSize) }
    override fun timeoutMode(timeoutMode: TimeoutMode): SyncListCollectionsIterable<T> = apply {
        wrapped.timeoutMode(timeoutMode)
    }

    override fun maxTime(maxTime: Long, timeUnit: TimeUnit): SyncListCollectionsIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    override fun filter(filter: Bson?): SyncListCollectionsIterable<T> = apply { wrapped.filter(filter) }
    override fun comment(comment: String?): SyncListCollectionsIterable<T> = apply { wrapped.comment(comment) }
    override fun comment(comment: BsonValue?): SyncListCollectionsIterable<T> = apply { wrapped.comment(comment) }
}
