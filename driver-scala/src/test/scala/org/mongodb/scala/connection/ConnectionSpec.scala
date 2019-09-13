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

package org.mongodb.scala.connection

import java.net.{ InetAddress, InetSocketAddress }

import com.mongodb.{ ServerAddress => JServerAddress }
import org.mongodb.scala.{ BaseSpec, ServerAddress }
import org.scalamock.scalatest.proxy.MockFactory

import scala.collection.JavaConverters._

class ConnectionSpec extends BaseSpec with MockFactory {

  "The connection namespace" should "have a AsynchronousSocketChannelStreamFactoryFactory companion" in {
    val asynchronousSocketChannelStreamFactoryFactory = AsynchronousSocketChannelStreamFactoryFactory()
    asynchronousSocketChannelStreamFactoryFactory shouldBe a[StreamFactoryFactory]
  }

  it should "have a NettyStreamFactoryFactory companion" in {
    val nettyStreamFactoryFactory = NettyStreamFactoryFactory()
    nettyStreamFactoryFactory shouldBe a[StreamFactoryFactory]
  }

  it should "have a ClusterSettings companion" in {
    val scalaSetting = ClusterSettings.builder().hosts(List(ServerAddress()).asJava).build()
    val javaSetting = com.mongodb.connection.ClusterSettings.builder().hosts(List(ServerAddress()).asJava).build()

    scalaSetting shouldBe a[com.mongodb.connection.ClusterSettings]
    scalaSetting should equal(javaSetting)
  }

  it should "have a ConnectionPoolSettings companion" in {
    val scalaSetting = ConnectionPoolSettings.builder.build()
    val javaSetting = com.mongodb.connection.ConnectionPoolSettings.builder().build()

    scalaSetting shouldBe a[com.mongodb.connection.ConnectionPoolSettings]
    scalaSetting should equal(javaSetting)
  }

  it should "have a ServerSettings companion" in {
    val scalaSetting = ServerSettings.builder.build()
    val javaSetting = com.mongodb.connection.ServerSettings.builder().build()

    scalaSetting shouldBe a[com.mongodb.connection.ServerSettings]
    scalaSetting should equal(javaSetting)
  }

  it should "have a SocketSettings companion" in {
    val scalaSetting = SocketSettings.builder.build()
    val javaSetting = com.mongodb.connection.SocketSettings.builder().build()

    scalaSetting shouldBe a[com.mongodb.connection.SocketSettings]
    scalaSetting should equal(javaSetting)
  }

  it should "have a SslSettings companion" in {
    val scalaSetting = SslSettings.builder.build()
    val javaSetting = com.mongodb.connection.SslSettings.builder().build()

    scalaSetting shouldBe a[com.mongodb.connection.SslSettings]
    scalaSetting should equal(javaSetting)
  }

  it should "have a ServerAddress companion" in {
    val scalaAddress = ServerAddress()
    val javaAddress = new JServerAddress()
    scalaAddress should equal(javaAddress)

    val scalaAddress1 = ServerAddress("localhost")
    val javaAddress1 = new JServerAddress("localhost")
    scalaAddress1 should equal(javaAddress1)

    val scalaAddress2 = ServerAddress("localhost")
    val javaAddress2 = new JServerAddress("localhost")
    scalaAddress2 should equal(javaAddress2)

    val inetAddress = InetAddress.getByName("localhost")
    val scalaAddress3 = ServerAddress(inetAddress)
    val javaAddress3 = new JServerAddress(inetAddress)
    scalaAddress3 should equal(javaAddress3)

    val scalaAddress4 = ServerAddress(inetAddress, 27017)
    val javaAddress4 = new JServerAddress(inetAddress, 27017)
    scalaAddress4 should equal(javaAddress4)

    val inetSocketAddress = new InetSocketAddress(inetAddress, 27017)
    val scalaAddress5 = ServerAddress(inetSocketAddress)
    val javaAddress5 = new JServerAddress(inetSocketAddress)
    scalaAddress5 should equal(javaAddress5)

    val scalaAddress6 = ServerAddress("localhost", 27017)
    val javaAddress6 = new JServerAddress("localhost", 27017)
    scalaAddress6 should equal(javaAddress6)
  }
}
