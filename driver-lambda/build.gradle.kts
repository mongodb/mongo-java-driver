/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("application")
    id("java-library")
    id("project.base")
    alias(libs.plugins.shadow)
}

application {
    mainClass = "com.mongodb.lambdatest.LambdaTestApp"
}

sourceSets {
    main {
        java { setSrcDirs(listOf("src/main")) }
        resources { setSrcDirs(listOf("src/resources")) }
    }
}

dependencies {
    implementation(project(":driver-sync"))
    implementation(project(":bson"))

    implementation(libs.aws.lambda.core)
    implementation(libs.aws.lambda.events)
    implementation(platform(libs.junit.bom))
    implementation(libs.bundles.junit)
}

tasks.withType<Javadoc>().configureEach {
    enabled = false
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "com.mongodb.lambdatest.LambdaTestApp"
    }
}
tasks.withType<ShadowJar>  {
    archiveBaseName.set("lambdatest")
    archiveVersion.set("")
}
