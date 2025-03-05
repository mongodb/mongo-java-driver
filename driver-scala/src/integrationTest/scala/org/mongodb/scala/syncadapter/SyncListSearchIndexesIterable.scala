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

import com.mongodb.ExplainVerbosity
import com.mongodb.client.ListSearchIndexesIterable
import com.mongodb.client.model.Collation
import org.bson.{ BsonValue, Document }
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo
import org.mongodb.scala.{ ListSearchIndexesObservable, TimeoutMode }

import java.util.concurrent.TimeUnit
import scala.reflect.ClassTag

case class SyncListSearchIndexesIterable[T](wrapped: ListSearchIndexesObservable[T])
    extends SyncMongoIterable[T]
    with ListSearchIndexesIterable[T] {

  override def name(indexName: String): ListSearchIndexesIterable[T] = {
    wrapped.name(indexName)
    this
  }

  override def allowDiskUse(allowDiskUse: java.lang.Boolean): ListSearchIndexesIterable[T] = {
    wrapped.allowDiskUse(allowDiskUse)
    this
  }

  override def batchSize(batchSize: Int): ListSearchIndexesIterable[T] = {
    wrapped.batchSize(batchSize)
    this
  }

  override def timeoutMode(timeoutMode: TimeoutMode): ListSearchIndexesIterable[T] = {
    wrapped.timeoutMode(timeoutMode)
    this
  }

  override def maxTime(maxTime: Long, timeUnit: TimeUnit): ListSearchIndexesIterable[T] = {
    wrapped.maxTime(maxTime, timeUnit)
    this
  }

  override def collation(collation: Collation): ListSearchIndexesIterable[T] = {
    wrapped.collation(collation)
    this
  }

  override def comment(comment: String): ListSearchIndexesIterable[T] = {
    wrapped.comment(comment)
    this
  }

  override def comment(comment: BsonValue): ListSearchIndexesIterable[T] = {
    wrapped.comment(comment)
    this
  }

  override def explain(): Document = wrapped.explain().toFuture().get()

  override def explain(verbosity: ExplainVerbosity): Document = wrapped.explain(verbosity).toFuture().get()

  override def explain[E](explainResultClass: Class[E]): E =
    wrapped
      .explain[E]()(DefaultsTo.overrideDefault[E, org.mongodb.scala.Document], ClassTag(explainResultClass))
      .toFuture()
      .get()

  override def explain[E](explainResultClass: Class[E], verbosity: ExplainVerbosity): E =
    wrapped
      .explain[E](verbosity)(DefaultsTo.overrideDefault[E, org.mongodb.scala.Document], ClassTag(explainResultClass))
      .toFuture()
      .get()

}
