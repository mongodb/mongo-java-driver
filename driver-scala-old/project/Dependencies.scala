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

import sbt.Keys.scalaVersion
import sbt._

object Dependencies {
  // Versions
  val scalaVersions           = Seq("2.11.12", "2.12.8", "2.13.0")
  val scalaCoreVersion        = "2.13.0"
  val mongodbDriverVersion    = "1.12.0"

  val scalaTestVersion        = "3.0.8"
  val scalaMockVersion        = "4.4.0"
  val logbackVersion          = "1.1.3"
  val reflectionsVersion      = "0.9.10"
  val javaxServeletApiVersion = "2.5"
  val nettyVersion            = "4.1.17.Final"

  val rxStreamsVersion        = "1.0.0"

  // Libraries
  val mongodbDriver = "org.mongodb" % "mongodb-driver-reactivestreams" % mongodbDriverVersion
  val scalaReflect  =  scalaVersion("org.scala-lang" % "scala-reflect" % _)

  // Test
  val scalaTest         = "org.scalatest" %% "scalatest" % scalaTestVersion % "it,test"
  val scalaMock         = "org.scalamock" %% "scalamock" % scalaMockVersion % "test"
  val logback           = "ch.qos.logback" % "logback-classic" % logbackVersion % "it,test"
  val reflections       = "org.reflections" % "reflections" % reflectionsVersion % "test"
  val javaxServeletApi  = "javax.servlet" % "servlet-api" % javaxServeletApiVersion % "test"
  val nettyTest         = "io.netty" % "netty-all" % nettyVersion % "test"
  val nettyIt             = "io.netty" % "netty-all" % nettyVersion % "it"

  // Examples
  val rxStreams         = "org.reactivestreams" % "reactive-streams" % rxStreamsVersion

  // Projects
  val coreDependencies     = Seq(mongodbDriver)
  val testDependencies     = Seq(scalaTest, scalaMock, logback, reflections, javaxServeletApi, nettyTest, nettyIt)
  val examplesDependencies = Seq(rxStreams, nettyTest)
}
