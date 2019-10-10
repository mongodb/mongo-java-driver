package org.mongodb.scala.syncadapter

import java.util.concurrent.TimeUnit

import com.mongodb.client.ListDatabasesIterable
import org.bson.conversions.Bson
import org.mongodb.scala.ListDatabasesObservable

case class SyncListDatabasesIterable[T](wrapped: ListDatabasesObservable[T])
    extends SyncMongoIterable[T]
    with ListDatabasesIterable[T] {
  override def maxTime(maxTime: Long, timeUnit: TimeUnit): ListDatabasesIterable[T] = {
    wrapped.maxTime(maxTime, timeUnit)
    this
  }

  override def batchSize(batchSize: Int): ListDatabasesIterable[T] = {
    wrapped.batchSize(batchSize)
    this
  }

  override def filter(filter: Bson): ListDatabasesIterable[T] = {
    wrapped.filter(filter)
    this
  }

  override def nameOnly(nameOnly: java.lang.Boolean): ListDatabasesIterable[T] = {
    wrapped.nameOnly(nameOnly)
    this
  }
}
