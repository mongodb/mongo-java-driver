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

import com.mongodb.{ ServerAddress, ServerCursor }
import com.mongodb.client.{ ChangeStreamIterable, MongoChangeStreamCursor }
import com.mongodb.client.model.Collation
import com.mongodb.client.model.changestream.{ ChangeStreamDocument, FullDocument }
import org.bson.{ BsonDocument, BsonTimestamp }
import org.mongodb.scala.ChangeStreamObservable

case class SyncChangeStreamIterable[T](wrapped: ChangeStreamObservable[T])
    extends SyncMongoIterable[ChangeStreamDocument[T]]
    with ChangeStreamIterable[T] {

  override def cursor: MongoChangeStreamCursor[ChangeStreamDocument[T]] = {
    val wrapped = super.cursor
    new MongoChangeStreamCursor[ChangeStreamDocument[T]]() {
      def getResumeToken = throw new UnsupportedOperationException
      def close(): Unit = wrapped.close()
      def hasNext: Boolean = wrapped.hasNext
      def next: ChangeStreamDocument[T] = wrapped.next
      def tryNext: ChangeStreamDocument[T] = wrapped.tryNext
      def getServerCursor: ServerCursor = wrapped.getServerCursor
      def getServerAddress: ServerAddress = wrapped.getServerAddress
    }
  }

  override def fullDocument(fullDocument: FullDocument): ChangeStreamIterable[T] = {
    wrapped.fullDocument(fullDocument)
    this
  }

  override def resumeAfter(resumeToken: BsonDocument): ChangeStreamIterable[T] = {
    wrapped.resumeAfter(resumeToken)
    this
  }

  override def batchSize(batchSize: Int): ChangeStreamIterable[T] = {
    wrapped.batchSize(batchSize)
    this
  }

  override def maxAwaitTime(maxAwaitTime: Long, timeUnit: TimeUnit): ChangeStreamIterable[T] = {
    wrapped.maxAwaitTime(maxAwaitTime, timeUnit)
    this
  }

  override def collation(collation: Collation): ChangeStreamIterable[T] = {
    wrapped.collation(collation)
    this
  }

  override def withDocumentClass[TDocument](clazz: Class[TDocument]) = throw new UnsupportedOperationException

  override def startAtOperationTime(startAtOperationTime: BsonTimestamp): ChangeStreamIterable[T] = {
    wrapped.startAtOperationTime(startAtOperationTime)
    this
  }

  override def startAfter(startAfter: BsonDocument): ChangeStreamIterable[T] = {
    wrapped.startAfter(startAfter)
    this
  }
}
