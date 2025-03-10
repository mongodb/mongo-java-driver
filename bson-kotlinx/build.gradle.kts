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
    alias(libs.plugins.kotlin.serialization)
}

base.archivesName.set("bson-kotlinx")

java {
    registerFeature("dateTimeSupport") { usingSourceSet(sourceSets["main"]) }
    registerFeature("jsonSupport") { usingSourceSet(sourceSets["main"]) }
}

dependencies {
    api(project(path = ":bson", configuration = "default"))
    implementation(platform(libs.kotlinx.serialization))
    implementation(libs.kotlinx.serialization.core)
    implementation(libs.kotlin.reflect)
    "dateTimeSupportImplementation"(libs.kotlinx.serialization.datetime)
    "jsonSupportImplementation"(libs.kotlinx.serialization.json)

    testImplementation(project(path = ":driver-core", configuration = "default"))
    testImplementation(libs.kotlinx.serialization.datetime)
    testImplementation(libs.kotlinx.serialization.json)
}

configureMavenPublication {
    pom {
        name.set("BSON Kotlinx")
        description.set("The BSON Codec for Kotlinx serialization")
        url.set("https://bsonspec.org")
    }

    suppressPomMetadataWarningsFor("dateTimeSupportApiElements")
    suppressPomMetadataWarningsFor("dateTimeSupportRuntimeElements")

    suppressPomMetadataWarningsFor("jsonSupportApiElements")
    suppressPomMetadataWarningsFor("jsonSupportRuntimeElements")
}

configureJarManifest { attributes["Automatic-Module-Name"] = "org.mongodb.bson.kotlinx" }
