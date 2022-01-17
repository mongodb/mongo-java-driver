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
package org.mongodb.scala.syncadapter

import java.util.concurrent.TimeUnit

import com.mongodb.client.ListCollectionsIterable
import org.bson.conversions.Bson
import org.mongodb.scala.ListCollectionsObservable

case class SyncListCollectionsIterable[T](wrapped: ListCollectionsObservable[T])
    extends SyncMongoIterable[T]
    with ListCollectionsIterable[T] {
  override def filter(filter: Bson): ListCollectionsIterable[T] = {
    wrapped.filter(filter)
    this
  }

  override def authorizedCollections(authorizedCollections: Boolean): ListCollectionsIterable[T] = {
    wrapped.authorizedCollections(authorizedCollections)
    this
  }

  override def maxTime(maxTime: Long, timeUnit: TimeUnit): ListCollectionsIterable[T] = {
    wrapped.maxTime(maxTime, timeUnit)
    this
  }

  override def batchSize(batchSize: Int): ListCollectionsIterable[T] = {
    wrapped.batchSize(batchSize)
    this
  }
}
