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

import com.mongodb.client.DistinctIterable
import com.mongodb.client.model.Collation
import org.bson.BsonValue
import org.bson.conversions.Bson
import org.mongodb.scala.{ DistinctObservable, TimeoutMode }

import java.util.concurrent.TimeUnit

case class SyncDistinctIterable[T](wrapped: DistinctObservable[T])
    extends SyncMongoIterable[T]
    with DistinctIterable[T] {
  override def filter(filter: Bson): DistinctIterable[T] = {
    wrapped.filter(filter)
    this
  }

  override def maxTime(maxTime: Long, timeUnit: TimeUnit): DistinctIterable[T] = {
    wrapped.maxTime(maxTime, timeUnit)
    this
  }

  override def batchSize(batchSize: Int): DistinctIterable[T] = {
    wrapped.batchSize(batchSize)
    this
  }

  override def timeoutMode(timeoutMode: TimeoutMode): DistinctIterable[T] = {
    wrapped.timeoutMode(timeoutMode)
    this
  }

  override def collation(collation: Collation): DistinctIterable[T] = {
    wrapped.collation(collation)
    this
  }

  override def comment(comment: String): DistinctIterable[T] = {
    wrapped.comment(comment)
    this
  }

  override def comment(comment: BsonValue): DistinctIterable[T] = {
    wrapped.comment(comment)
    this
  }

  override def hint(hint: Bson): DistinctIterable[T] = {
    wrapped.hint(hint)
    this
  }

  override def hintString(hint: String): DistinctIterable[T] = {
    wrapped.hintString(hint)
    this
  }

}
