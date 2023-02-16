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

package org.mongodb.scala.syncadapter;

import java.util.function.Consumer

import com.mongodb.Function
import com.mongodb.client.{ MongoCursor, MongoIterable }
import org.mongodb.scala.Observable

import scala.concurrent.Await
import scala.language.reflectiveCalls

trait SyncMongoIterable[T] extends MongoIterable[T] {

  val wrapped: Observable[T]

  override def iterator(): MongoCursor[T] = cursor()

  override def cursor(): MongoCursor[T] = SyncMongoCursor[T](wrapped)

  override def first(): T = Await.result(wrapped.head(), WAIT_DURATION)

  override def map[U](mapper: Function[T, U]) = throw new UnsupportedOperationException

  override def forEach(action: Consumer[_ >: T]): Unit = {
    use(cursor())(localCursor => while (localCursor.hasNext) action.accept(localCursor.next()))
  }

  override def into[A <: java.util.Collection[_ >: T]](target: A): A = {
    use(cursor())(localCursor => while (localCursor.hasNext) target.add(localCursor.next()))
    target
  }

  def use[A <: { def close(): Unit }, B](resource: A)(code: A => B): B = {
    try {
      code(resource)
    } finally {
      resource.close()
    }
  }
}
