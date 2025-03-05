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

plugins {
    id("application")
    id("java-library")
    id("project.base")
    id("conventions.test-artifacts")
}

application {
    mainClass = "com.mongodb.workload.WorkloadExecutor"
}

sourceSets {
    main {
        java { setSrcDirs(listOf("src/main")) }
        resources { setSrcDirs(listOf("src/resources")) }
    }
}

dependencies {
    implementation(project(":driver-sync"))
    implementation(project(path = ":driver-core", configuration = "testArtifacts"))
    implementation(project(path = ":driver-sync", configuration = "testArtifacts"))
    implementation(platform(libs.junit.bom))
    implementation(libs.bundles.junit.vintage)
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
        attributes["Main-Class"] = "com.mongodb.workload.WorkloadExecutor"
    }
}
