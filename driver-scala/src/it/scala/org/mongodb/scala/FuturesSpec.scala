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

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{ Failure, Success }

trait FuturesSpec extends ScalaFutures {

  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(60, Seconds), interval = Span(5, Millis))

  implicit def observableToFuture[TResult](observable: Observable[TResult]): Future[Seq[TResult]] =
    observable.toFuture()

  implicit def observableToFutureConcept[T](observable: Observable[T]): FutureConcept[Seq[T]] = {
    val future: Future[Seq[T]] = observable
    new FutureConcept[Seq[T]] {
      def eitherValue: Option[Either[Throwable, Seq[T]]] = {
        future.value.map {
          case Success(o) => Right(o)
          case Failure(e) => Left(e)
        }
      }
      def isExpired: Boolean = false

      // Scala Futures themselves don't support the notion of a timeout
      def isCanceled: Boolean = false // Scala Futures don't seem to be cancelable either
    }
  }

  implicit def observableToFuture[TResult](observable: SingleObservable[TResult]): Future[TResult] =
    observable.toFuture()
  implicit def observableToFutureConcept[T](observable: SingleObservable[T]): FutureConcept[T] = {
    val future: Future[T] = observable.toFuture()
    new FutureConcept[T] {
      def eitherValue: Option[Either[Throwable, T]] = {
        future.value.map {
          case Success(o) => Right(o)
          case Failure(e) => Left(e)
        }
      }
      def isExpired: Boolean = false

      // Scala Futures themselves don't support the notion of a timeout
      def isCanceled: Boolean = false // Scala Futures don't seem to be cancelable either
    }
  }

}
