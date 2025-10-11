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
import ProjectExtensions.configureJarManifest
import ProjectExtensions.configureMavenPublication

plugins {
    id("project.java")
    id("conventions.test-artifacts")
    id("conventions.test-artifacts-runtime-dependencies")
    id("conventions.test-include-optionals")
    id("conventions.testing-mockito")
    id("conventions.testing-junit")
    id("conventions.testing-spock-exclude-slow")
}

base.archivesName.set("mongodb-driver-reactivestreams")

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(project(path = ":driver-core", configuration = "default"))
    api(libs.reactive.streams)
    implementation(platform(libs.project.reactor.bom))
    implementation(libs.project.reactor.core)
    compileOnly(project(path = ":mongodb-crypt", configuration = "default"))

    testImplementation(libs.project.reactor.test)
    testImplementation(project(path = ":driver-sync", configuration = "default"))
    testImplementation(project(path = ":bson", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-core", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-sync", configuration = "testArtifacts"))

    // Reactive Streams TCK testing
    testImplementation(libs.reactive.streams.tck)

    // Tracing
    testImplementation(libs.micrometer.tracing.integration.test) { exclude(group = "org.junit.jupiter") }
}

configureMavenPublication {
    pom {
        name.set("The MongoDB Reactive Streams Driver")
        description.set("A Reactive Streams implementation of the MongoDB Java driver")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "org.mongodb.driver.reactivestreams"
    attributes["Bundle-SymbolicName"] = "org.mongodb.driver-reactivestreams"
    attributes["Import-Package"] =
        listOf(
                "com.mongodb.crypt.capi.*;resolution:=optional",
                "com.mongodb.internal.crypt.capi.*;resolution:=optional",
                "*" // import all that is not excluded or modified before
                )
            .joinToString(",")
}

sourceSets { test { java { setSrcDirs(listOf("src/test/tck")) } } }

// Reactive Streams TCK uses TestNG
tasks.register("tckTest", Test::class) {
    useTestNG()
    maxParallelForks = 1
    isScanForTestClasses = false

    binaryResultsDirectory.set(layout.buildDirectory.dir("$name-results/binary"))
    reports.html.outputLocation.set(layout.buildDirectory.dir("reports/$name"))
    reports.junitXml.outputLocation.set(layout.buildDirectory.dir("reports/$name-results"))
}
