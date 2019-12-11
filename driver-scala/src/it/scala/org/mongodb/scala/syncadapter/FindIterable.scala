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

import java.util.concurrent.TimeUnit

import com.mongodb.CursorType
import com.mongodb.client.FindIterable
import com.mongodb.client.model.Collation
import org.bson.conversions.Bson
import org.mongodb.scala.FindObservable

case class SyncFindIterable[T](wrapped: FindObservable[T]) extends SyncMongoIterable[T] with FindIterable[T] {
  override def filter(filter: Bson): FindIterable[T] = {
    wrapped.filter(filter)
    this
  }

  override def limit(limit: Int): FindIterable[T] = {
    wrapped.limit(limit)
    this
  }

  override def skip(skip: Int): FindIterable[T] = {
    wrapped.skip(skip)
    this
  }

  override def maxTime(maxTime: Long, timeUnit: TimeUnit): FindIterable[T] = {
    wrapped.maxTime(maxTime, timeUnit)
    this
  }

  override def maxAwaitTime(maxAwaitTime: Long, timeUnit: TimeUnit): FindIterable[T] = {
    wrapped.maxAwaitTime(maxAwaitTime, timeUnit)
    this
  }

  override def projection(projection: Bson): FindIterable[T] = {
    wrapped.projection(projection)
    this
  }

  override def sort(sort: Bson): FindIterable[T] = {
    wrapped.sort(sort)
    this
  }

  override def noCursorTimeout(noCursorTimeout: Boolean): FindIterable[T] = {
    wrapped.noCursorTimeout(noCursorTimeout)
    this
  }

  override def oplogReplay(oplogReplay: Boolean): FindIterable[T] = {
    wrapped.oplogReplay(oplogReplay)
    this
  }

  override def partial(partial: Boolean): FindIterable[T] = {
    wrapped.partial(partial)
    this
  }

  override def cursorType(cursorType: CursorType): FindIterable[T] = {
    wrapped.cursorType(cursorType)
    this
  }

  override def batchSize(batchSize: Int): FindIterable[T] = {
    wrapped.batchSize(batchSize)
    this
  }

  override def collation(collation: Collation): FindIterable[T] = {
    wrapped.collation(collation)
    this
  }

  override def comment(comment: String): FindIterable[T] = {
    wrapped.comment(comment)
    this
  }

  override def hint(hint: Bson): FindIterable[T] = {
    wrapped.hint(hint)
    this
  }

  override def hintString(hint: String): FindIterable[T] = {
    wrapped.hintString(hint)
    this
  }

  override def max(max: Bson): FindIterable[T] = {
    wrapped.max(max)
    this
  }

  override def min(min: Bson): FindIterable[T] = {
    wrapped.min(min)
    this
  }

  override def returnKey(returnKey: Boolean): FindIterable[T] = {
    wrapped.returnKey(returnKey)
    this
  }

  override def showRecordId(showRecordId: Boolean): FindIterable[T] = {
    wrapped.showRecordId(showRecordId)
    this
  }
}
