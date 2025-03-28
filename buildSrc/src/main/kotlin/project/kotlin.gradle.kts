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

import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile

plugins {
    kotlin("jvm")
    id("project.base")
    id("conventions.bnd")
    id("conventions.detekt")
    id("conventions.dokka")
    id("conventions.optional")
    id("conventions.publishing")
    id("conventions.spotbugs")
    id("conventions.spotless")
    id("conventions.testing-integration")
    id("conventions.testing-junit")
}

/* Compiling */
logger.info("Compiling ${project.name} using JDK${DEFAULT_JAVA_VERSION}")

kotlin {
    explicitApi()
    jvmToolchain(DEFAULT_JAVA_VERSION)
}

tasks.withType<KotlinJvmCompile> { compilerOptions { jvmTarget = JvmTarget.JVM_1_8 } }

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8

    withSourcesJar()
    withJavadocJar()
}

dependencies {
    "optionalApi"(libs.slf4j)

    // Align versions of all Kotlin components
    implementation(platform(libs.kotlin.bom))
    implementation(libs.kotlin.stdlib.jdk8)

    testImplementation(libs.kotlin.reflect)
    testImplementation(libs.junit.kotlin)
    testImplementation(libs.bundles.mockito.kotlin)
    testImplementation(libs.assertj)
    testImplementation(libs.classgraph)

    "integrationTestImplementation"(libs.junit.kotlin)
}
