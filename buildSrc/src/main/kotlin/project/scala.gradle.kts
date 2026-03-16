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
package project

import ProjectExtensions.configureMavenPublication
import ProjectExtensions.scalaVersion

plugins {
    id("scala")
    id("project.base")
    id("conventions.bnd")
    id("conventions.optional")
    id("conventions.publishing")
    id("conventions.scaladoc")
    id("conventions.spotless")
    id("conventions.testing-junit")
    id("conventions.testing-integration")
}

group = "org.mongodb.scala"

val scalaVersion: String by lazy { project.scalaVersion() }

sourceSets["integrationTest"].scala.srcDir("src/integrationTest/scala")

tasks.register("scalaCheck") {
    description = "Runs all the Scala checks"
    group = "verification"

    dependsOn("clean", "compileTestScala", "check")
    tasks.findByName("check")?.mustRunAfter("clean")
}

tasks.withType<Test> {
    doFirst { println("Running Test task using scala version: $scalaVersion") }
    useJUnitPlatform()
}

tasks.named<Delete>("clean") { delete.add(rootProject.file("build/docs/")) }

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8

    withSourcesJar()
    withJavadocJar()
}

afterEvaluate {
    configureMavenPublication { artifactId = "${base.archivesName.get()}_${scalaVersion}" }

    // ============================================
    //     Scala version specific configuration
    // ============================================
    val compileOptions: MutableList<String> = mutableListOf()
    when (scalaVersion) {
        "3" -> {
            dependencies {
                api(libs.bundles.scala.v3)

                testImplementation(libs.bundles.scala.test.v3)
            }
            sourceSets { main { scala { setSrcDirs(listOf("src/main/scala", "src/main/scala-3")) } } }
            sourceSets { test { scala { setSrcDirs(listOf("src/test/scala", "src/test/scala-3")) } } }

            compileOptions.addAll(listOf("-release:8", "-Xcheck-macros", "-Ycheck:all"))
        }
        "2.13" -> {
            dependencies {
                api(libs.bundles.scala.v2.v13)

                testImplementation(libs.bundles.scala.test.v2.v13)
            }
            sourceSets { main { scala { setSrcDirs(listOf("src/main/scala", "src/main/scala-2", "src/main/scala-2.13")) } } }
            sourceSets { test { scala { setSrcDirs(listOf("src/test/scala", "src/test/scala-2", "src/test/scala-2.13")) } } }

            compileOptions.addAll(
                listOf(
                    "-release:8",
                    "-feature",
                    "-unchecked",
                    "-language:reflectiveCalls",
                    "-Wconf:cat=deprecation:ws",
                    "-Xlint:strict-unsealed-patmat"))
        }
        "2.12" -> {
            dependencies {
                api(libs.bundles.scala.v2.v12)

                testImplementation(libs.bundles.scala.test.v2.v12)
            }
            sourceSets { main { scala { setSrcDirs(listOf("src/main/scala", "src/main/scala-2", "src/main/scala-2.13-")) } } }
            sourceSets { test { scala { setSrcDirs(listOf("src/test/scala", "src/test/scala-2", "src/test/scala-2.13-")) } } }

            compileOptions.add("-target:jvm-1.8")
        }
        "2.11" -> {
            dependencies {
                api(libs.bundles.scala.v2.v11)

                testImplementation(libs.bundles.scala.test.v2.v11)
            }
            // Reuse the scala-2.12 source as its compatible.
            sourceSets { main { scala { setSrcDirs(listOf("src/main/scala", "src/main/scala-2", "src/main/scala-2.13-")) } } }
            sourceSets { test { scala { setSrcDirs(listOf("src/test/scala", "src/test/scala-2", "src/test/scala-2.13-")) } } }

            compileOptions.addAll(listOf("-target:jvm-1.8", "-Xexperimental"))
        }
    }

    tasks.withType<ScalaCompile> {
        doFirst { println("Compiling using scala version: $scalaVersion") }

        scalaCompileOptions.isDeprecation = false
        scalaCompileOptions.additionalParameters = compileOptions
    }
}
