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
import sbt._

object Resolvers {
  // Repositories
  val sonatypeSnaps = "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  val sonatypeRels  = "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases"

  val typeSafeSnaps = "TypeSafe snapshots" at "http://repo.typesafe.com/typesafe/snapshots"
  val typeSafeRels  = "TypeSafe releases" at "http://repo.typesafe.com/typesafe/releases"

  val localMaven    = "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

  val mongoScalaResolvers = Seq(sonatypeSnaps, localMaven, sonatypeRels, typeSafeSnaps, typeSafeRels)
}
