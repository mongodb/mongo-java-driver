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

package conventions

import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    id("scala")
    id("conventions.publishing")
    id("conventions.spotless")
    id("conventions.testing")
}

group = "org.mongodb.scala"

tasks {
    compileScala { dependsOn("spotlessApply") }
    compileTestScala { dependsOn("spotlessApply") }
}

val scalaVersion: String by lazy { extra.get("scalaVersion") as String? ?: "unknown" }

tasks.withType<GenerateModuleMetadata> { enabled = false }

afterEvaluate {
    tasks.withType<ScalaCompile> {
        scalaCompileOptions.isDeprecation = false
        val compileOptions = mutableListOf("-target:jvm-1.8")
        compileOptions.addAll(when (scalaVersion) {
            "2.13" -> listOf(
                "-feature",
                "-unchecked",
                "-language:reflectiveCalls",
                "-Wconf:cat=deprecation:ws,any:e",
                "-Xlint:strict-unsealed-patmat"
            )
            "2.11" -> listOf("-Xexperimental")
            else -> emptyList()
        })
        scalaCompileOptions.additionalParameters = compileOptions
    }
}


// ===================
//     Scala checks
// ===================
tasks.register("scalaCheck") {
    description = "Runs all the Scala checks"
    group = "verification"

    dependsOn("clean", "compileTestScala", "check")
    tasks.findByName("check")?.mustRunAfter("clean")
}

// ===================
//     Scala testing
// ===================
tasks.withType<Test> {
    doFirst { println("Running Test task using scala version: $scalaVersion") }
    useJUnitPlatform { includeEngines("scalatest") }
}

// ===================
//     Scala docs
// ===================

tasks.withType<ScalaDoc> {
    group = "documentation"

    afterEvaluate {
        destinationDir = file("${rootProject.buildDir.path}/docs/${project.base.archivesName.get()}")
    }
}

rootProject.tasks.named("docs") {
    dependsOn(tasks.findByName("scaladoc"))
}

tasks.named<Delete>("clean") {
    delete.add("${rootProject.buildDir.path}/docs")
}
