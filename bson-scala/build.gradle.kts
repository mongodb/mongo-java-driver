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
import config.Extensions.scalaVersion
import config.Extensions.setAll

plugins { id("conventions.scala") }

base.archivesName.set("mongo-scala-bson")

val scalaVersion: String = project.scalaVersion()

extra.setAll(
    mapOf(
        "mavenName" to "Mongo Scala Bson Library",
        "mavenDescription" to "A Scala wrapper / extension to the bson library",
        "automaticModuleName" to "org.mongodb.bson.scala",
        "importPackage" to "!scala.*, *",
        "scalaVersion" to scalaVersion,
        "mavenArtifactId" to "${base.archivesName.get()}_${scalaVersion}"))

dependencies { implementation(project(path = ":bson", configuration = "default")) }

// ============================================
//     Scala version specific configuration
// ============================================
when (scalaVersion) {
    "2.13" -> {
        dependencies {
            implementation(libs.bundles.scala.v2.v13)

            testImplementation(libs.bundles.scala.test.v2.v13)
        }
        sourceSets { main { scala { setSrcDirs(listOf("src/main/scala", "src/main/scala-2.13")) } } }
    }
    "2.12" -> {
        dependencies {
            implementation(libs.bundles.scala.v2.v12)

            testImplementation(libs.bundles.scala.test.v2.v12)
        }
        sourceSets { main { scala { setSrcDirs(listOf("src/main/scala", "src/main/scala-2.12")) } } }
    }
    "2.11" -> {
        dependencies {
            implementation(libs.bundles.scala.v2.v11)

            testImplementation(libs.bundles.scala.test.v2.v11)
        }
        sourceSets { main { scala { setSrcDirs(listOf("src/main/scala", "src/main/scala-2.12")) } } }
    }
}
