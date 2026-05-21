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
plugins {
    id("java-library")
    id("project.base")
    id("checkstyle")
    id("conventions.testing-base")
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.jupiter.platform.launcher)
    testImplementation(libs.assertj)
    testImplementation(libs.felix.framework)
    testImplementation(libs.reactive.streams)
    testImplementation(platform(libs.project.reactor.bom))
    testImplementation(libs.project.reactor.core)
}

tasks.test {
    dependsOn(":bson:jar", ":driver-core:jar", ":driver-sync:jar", ":driver-reactive-streams:jar")
    systemProperty("projectRoot", rootProject.projectDir.absolutePath)
}
