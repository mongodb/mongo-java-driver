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

plugins {
    id("conventions.scala")
    id("conventions.integrationTesting")
}

val scalaVersion: String = project.scalaVersion()

base.archivesName.set("mongo-scala-driver")

extra.setAll(
    mapOf(
        "mavenName" to "Mongo Scala Driver",
        "mavenDescription" to "A Scala wrapper of the MongoDB Reactive Streams Java driver",
        "automaticModuleName" to "org.mongodb.bson.scala",
        "importPackage" to "!scala.*, *",
        "scalaVersion" to scalaVersion,
        "mavenArtifactId" to "${base.archivesName.get()}_${scalaVersion}"))

dependencies {
    implementation(project(path = ":bson-scala", configuration = "default"))
    implementation(project(path = ":driver-reactive-streams", configuration = "default"))

    "integrationTestImplementation"(project(path = ":driver-sync"))
    "integrationTestImplementation"(project(path = ":driver-reactive-streams"))
    "integrationTestImplementation"(project(path = ":bson", configuration = "testArtifacts"))
    "integrationTestImplementation"(project(path = ":driver-core", configuration = "testArtifacts"))
    "integrationTestImplementation"(project(path = ":driver-sync", configuration = "testArtifacts"))
    "integrationTestImplementation"(project(path = ":driver-reactive-streams", configuration = "testArtifacts"))
}

// ===================
//     Scala docs
// ===================

tasks.withType<ScalaDoc>().forEach {
    // Include bson-scala source for main scaladoc
    project(":bson-scala").tasks.withType<ScalaDoc>().forEach { bsonScala -> it.source += bsonScala.source }
    it.scalaDocOptions.additionalParameters = listOf("-doc-root-content", "${project.rootDir}/driver-scala/rootdoc.txt")
}

// ============================================
//     Scala version specific configuration
// ============================================
when (scalaVersion) {
    "2.13" -> {
        dependencies {
            implementation(libs.bundles.scala.v2.v13)

            testImplementation(libs.bundles.scala.test.v2.v13)
        }
    }
    "2.12" -> {
        dependencies {
            implementation(libs.bundles.scala.v2.v12)

            testImplementation(libs.bundles.scala.test.v2.v12)
        }
    }
    "2.11" -> {
        dependencies {
            implementation(libs.bundles.scala.v2.v11)

            testImplementation(libs.bundles.scala.test.v2.v11)
        }
    }
}
