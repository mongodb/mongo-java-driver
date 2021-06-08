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

package org.mongodb.scala

import com.mongodb.client.AbstractMainTransactionsTest
import org.bson.{ BsonArray, BsonDocument }
import org.junit.{ After, Before }
import org.mongodb.scala.syncadapter.SyncMongoClient
import com.mongodb.reactivestreams.client.MainTransactionsTest.SESSION_CLOSE_TIMING_SENSITIVE_TESTS
import com.mongodb.reactivestreams.client.syncadapter.{ SyncMongoClient => JSyncMongoClient }

class MainTransactionsTest(
    val filename: String,
    val description: String,
    val databaseName: String,
    val collectionName: String,
    val data: BsonArray,
    val definition: BsonDocument,
    val skipTest: Boolean
) extends AbstractMainTransactionsTest(
      filename,
      description,
      databaseName,
      collectionName,
      data,
      definition,
      skipTest
    ) {
  override protected def createMongoClient(settings: com.mongodb.MongoClientSettings) =
    SyncMongoClient(MongoClient(settings))

  @Before def before(): Unit = {
    if (SESSION_CLOSE_TIMING_SENSITIVE_TESTS.contains(getDescription))
      JSyncMongoClient.enableSleepAfterSessionClose(256)
  }

  @After def after(): Unit = JSyncMongoClient.disableSleep()
}
