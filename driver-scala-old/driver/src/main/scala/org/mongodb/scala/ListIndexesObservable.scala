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

package org.mongodb.scala

import java.util.concurrent.TimeUnit

import com.mongodb.reactivestreams.client.ListIndexesPublisher

import scala.concurrent.duration.Duration

/**
 * Observable interface for ListIndexes.
 *
 * @param wrapped the underlying java ListIndexesObservable
 * @tparam TResult The type of the result.
 * @since 1.0
 */
case class ListIndexesObservable[TResult](wrapped: ListIndexesPublisher[TResult]) extends Observable[TResult] {

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[http://docs.mongodb.org/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): ListIndexesObservable[TResult] = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   * @since 2.7
   */
  def batchSize(batchSize: Int): ListIndexesObservable[TResult] = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Helper to return a single observable limited to the first result.
   *
   * @return a single observable which will the first result.
   * @since 4.0
   */
  def first(): SingleObservable[TResult] = wrapped.first()

  override def subscribe(observer: Observer[_ >: TResult]): Unit = wrapped.subscribe(observer)
}
