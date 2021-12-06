/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.scala

import com.mongodb.ClusterFixture.getServerApi
import org.mongodb.scala.syncadapter.WAIT_DURATION

import scala.concurrent.Await
import scala.util.{ Properties, Try }

object TestMongoClientHelper {
  private val DEFAULT_URI: String = "mongodb://localhost:27017/"
  private val MONGODB_URI_SYSTEM_PROPERTY_NAME: String = "org.mongodb.test.uri"

  val mongoClientURI: String = {
    val uri = Properties.propOrElse(MONGODB_URI_SYSTEM_PROPERTY_NAME, DEFAULT_URI)
    if (!uri.isBlank) uri else DEFAULT_URI
  }
  val connectionString: ConnectionString = ConnectionString(mongoClientURI)

  def mongoClientSettingsBuilder: MongoClientSettings.Builder = {
    val builder = MongoClientSettings.builder().applyConnectionString(connectionString)
    if (getServerApi != null) {
      builder.serverApi(getServerApi)
    }
    builder
  }

  val mongoClientSettings: MongoClientSettings = mongoClientSettingsBuilder.build()
  val mongoClient: MongoClient = MongoClient(mongoClientSettings)

  def isMongoDBOnline: Boolean = {
    Try(Await.result(TestMongoClientHelper.mongoClient.listDatabaseNames().toFuture(), WAIT_DURATION)).isSuccess
  }

  def hasSingleHost: Boolean = {
    TestMongoClientHelper.connectionString.getHosts.size() == 1
  }

  Runtime.getRuntime.addShutdownHook(new ShutdownHook())

  private[mongodb] class ShutdownHook extends Thread {
    override def run() {
      mongoClient.close()
    }
  }
}
