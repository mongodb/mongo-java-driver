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

import com.mongodb.Block
import org.bson.codecs.configuration.CodecRegistries._
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.DocumentCodecProvider
import org.mongodb.scala.connection.ConnectionPoolSettings.Builder
import org.scalamock.scalatest.proxy.MockFactory
import org.mongodb.scala.connection._

class MongoClientSettingsSpec extends BaseSpec with MockFactory {

  "MongoClientSettings" should "default with the Scala Codec Registry" in {
    MongoClientSettings.builder().build().getCodecRegistry should equal(DEFAULT_CODEC_REGISTRY)
  }

  it should "keep the default Scala Codec Registry in no codec registry is set" in {
    val settings = MongoClientSettings.builder().readPreference(ReadPreference.nearest()).build()
    MongoClientSettings.builder(settings).build().getCodecRegistry should equal(DEFAULT_CODEC_REGISTRY)
  }

  it should "use a none default Codec Registry if set" in {
    val codecRegistry = fromProviders(DocumentCodecProvider())
    val settings = MongoClientSettings.builder().codecRegistry(codecRegistry).build()
    MongoClientSettings.builder(settings).build().getCodecRegistry should equal(codecRegistry)
  }

  it should "allow local Builder types" in {
    MongoClientSettings
      .builder()
      .applyToClusterSettings(new Block[ClusterSettings.Builder] {
        override def apply(t: ClusterSettings.Builder): Unit = {}
      })
      .applyToConnectionPoolSettings(new Block[ConnectionPoolSettings.Builder] {
        override def apply(t: Builder): Unit = {}
      })
      .applyToServerSettings(new Block[ServerSettings.Builder] {
        override def apply(t: ServerSettings.Builder): Unit = {}
      })
      .applyToSocketSettings(new Block[SocketSettings.Builder] {
        override def apply(t: SocketSettings.Builder): Unit = {}
      })
      .applyToSslSettings(new Block[SslSettings.Builder] {
        override def apply(t: SslSettings.Builder): Unit = {}
      })
      .build()
  }

}
