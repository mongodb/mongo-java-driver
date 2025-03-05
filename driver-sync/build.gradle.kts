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
    id("conventions.test-include-optionals")
    id("conventions.testing-mockito")
    id("conventions.testing-spock-exclude-slow")
}

base.archivesName.set("mongodb-driver-sync")

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(project(path = ":driver-core", configuration = "default"))
    compileOnly(project(path = ":mongodb-crypt", configuration = "default"))

    testImplementation(project(path = ":bson", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-core", configuration = "testArtifacts"))

    // lambda testing
    testImplementation(libs.aws.lambda.core)
}

configureMavenPublication {
    pom {
        name.set("MongoDB Driver")
        description.set("The MongoDB Synchronous Driver")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "org.mongodb.driver.sync.client"
    attributes["Bundle-SymbolicName"] = "org.mongodb.driver-sync"
    attributes["Import-Package"] =
        listOf(
                "com.mongodb.crypt.capi.*;resolution:=optional",
                "com.mongodb.internal.crypt.capi.*;resolution:=optional",
                "*",
            )
            .joinToString(",")
}
