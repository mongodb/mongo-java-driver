/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala.syncadapter

import com.mongodb.client.ListIndexesIterable
import org.mongodb.scala.ListIndexesObservable
import org.mongodb.scala.bson.BsonValue

import java.util.concurrent.TimeUnit

case class SyncListIndexesIterable[T](wrapped: ListIndexesObservable[T])
    extends SyncMongoIterable[T]
    with ListIndexesIterable[T] {
  override def maxTime(maxTime: Long, timeUnit: TimeUnit): ListIndexesIterable[T] = {
    wrapped.maxTime(maxTime, timeUnit)
    this
  }

  override def batchSize(batchSize: Int): ListIndexesIterable[T] = {
    wrapped.batchSize(batchSize)
    this
  }

  override def comment(comment: String): ListIndexesIterable[T] = {
    wrapped.comment(comment)
    this
  }

  override def comment(comment: BsonValue): ListIndexesIterable[T] = {
    wrapped.comment(comment)
    this
  }

}
