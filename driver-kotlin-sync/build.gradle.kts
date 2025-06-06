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
    id("project.kotlin")
    id("conventions.test-artifacts")
    id("conventions.test-artifacts-runtime-dependencies")
}

base.archivesName.set("mongodb-driver-kotlin-sync")

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(project(path = ":driver-sync", configuration = "default"))
    implementation(project(path = ":bson-kotlin", configuration = "default"))

    integrationTestImplementation(project(path = ":bson", configuration = "testArtifacts"))
    integrationTestImplementation(project(path = ":driver-sync", configuration = "testArtifacts"))
    integrationTestImplementation(project(path = ":driver-core", configuration = "testArtifacts"))
}

configureMavenPublication {
    pom {
        name.set("MongoDB Kotlin Driver")
        description.set("The MongoDB Kotlin Driver")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "org.mongodb.driver.kotlin.sync"
    attributes["Bundle-SymbolicName"] = "org.mongodb.mongodb-driver-kotlin-sync"
}
