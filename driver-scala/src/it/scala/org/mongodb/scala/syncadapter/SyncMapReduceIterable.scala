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

import com.mongodb.client.MapReduceIterable
import com.mongodb.client.model.{ Collation, MapReduceAction }
import org.bson.conversions.Bson
import org.mongodb.scala.MapReduceObservable

case class SyncMapReduceIterable[T](wrapped: MapReduceObservable[T])
    extends SyncMongoIterable[T]
    with MapReduceIterable[T] {
  override def toCollection(): Unit = wrapped.toCollection().toFuture().get()

  override def collectionName(collectionName: String): MapReduceIterable[T] = {
    wrapped.collectionName(collectionName)
    this
  }

  override def finalizeFunction(finalizeFunction: String): MapReduceIterable[T] = {
    wrapped.finalizeFunction(finalizeFunction)
    this
  }

  override def scope(scope: Bson): MapReduceIterable[T] = {
    wrapped.scope(scope)
    this
  }

  override def sort(sort: Bson): MapReduceIterable[T] = {
    wrapped.sort(sort)
    this
  }

  override def filter(filter: Bson): MapReduceIterable[T] = {
    wrapped.filter(filter)
    this
  }

  override def limit(limit: Int): MapReduceIterable[T] = {
    wrapped.limit(limit)
    this
  }

  override def jsMode(jsMode: Boolean): MapReduceIterable[T] = {
    wrapped.jsMode(jsMode)
    this
  }

  override def verbose(verbose: Boolean): MapReduceIterable[T] = {
    wrapped.verbose(verbose)
    this
  }

  override def maxTime(maxTime: Long, timeUnit: TimeUnit): MapReduceIterable[T] = {
    wrapped.maxTime(maxTime, timeUnit)
    this
  }

  override def action(action: MapReduceAction): MapReduceIterable[T] = {
    wrapped.action(action)
    this
  }

  override def databaseName(databaseName: String): MapReduceIterable[T] = {
    wrapped.databaseName(databaseName)
    this
  }

  override def sharded(sharded: Boolean): MapReduceIterable[T] = {
    wrapped.sharded(sharded)
    this
  }

  override def nonAtomic(nonAtomic: Boolean): MapReduceIterable[T] = {
    wrapped.nonAtomic(nonAtomic)
    this
  }

  override def batchSize(batchSize: Int): MapReduceIterable[T] = {
    wrapped.batchSize(batchSize)
    this
  }

  override def bypassDocumentValidation(bypassDocumentValidation: java.lang.Boolean): MapReduceIterable[T] = {
    wrapped.bypassDocumentValidation(bypassDocumentValidation)
    this
  }

  override def collation(collation: Collation): MapReduceIterable[T] = {
    wrapped.collation(collation)
    this
  }
}
