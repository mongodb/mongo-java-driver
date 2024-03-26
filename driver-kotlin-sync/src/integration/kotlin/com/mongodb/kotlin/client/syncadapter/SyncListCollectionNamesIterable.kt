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

import com.mongodb.client.ListCollectionNamesIterable as JListCollectionsIterable
import com.mongodb.kotlin.client.ListCollectionNamesIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.conversions.Bson

internal class SyncListCollectionNamesIterable(val wrapped: ListCollectionNamesIterable) :
    JListCollectionsIterable, SyncMongoIterable<String>(wrapped) {

    override fun batchSize(batchSize: Int): SyncListCollectionNamesIterable = apply { wrapped.batchSize(batchSize) }

    @Suppress("DEPRECATION")
    @Deprecated("Prefer using the operation execution timeout configuration option", level = DeprecationLevel.HIDDEN)
    override fun maxTime(maxTime: Long, timeUnit: TimeUnit): SyncListCollectionNamesIterable = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    override fun filter(filter: Bson?): SyncListCollectionNamesIterable = apply { wrapped.filter(filter) }

    override fun comment(comment: String?): SyncListCollectionNamesIterable = apply { wrapped.comment(comment) }
    override fun comment(comment: BsonValue?): SyncListCollectionNamesIterable = apply { wrapped.comment(comment) }

    override fun authorizedCollections(authorizedCollections: Boolean): SyncListCollectionNamesIterable = apply {
        wrapped.authorizedCollections(authorizedCollections)
    }
}
