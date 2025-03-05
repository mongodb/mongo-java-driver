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
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins { id("project.kotlin") }

base.archivesName.set("mongodb-driver-kotlin-extensions")

dependencies {
    api(project(path = ":driver-core", configuration = "default"))

    // Some extensions require higher API like MongoCollection which are defined in the sync &
    // coroutine Kotlin driver
    optionalImplementation(project(path = ":driver-kotlin-sync", configuration = "default"))
    optionalImplementation(project(path = ":driver-kotlin-coroutine", configuration = "default"))

    testImplementation(platform(libs.kotlinx.serialization))
    testImplementation(libs.kotlinx.serialization.core)
}

configureMavenPublication {
    pom {
        name.set("MongoDB Kotlin Driver Extensions")
        description.set("The MongoDB Kotlin Driver Extensions")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "org.mongodb.driver.kotlin.extensions"
    attributes["Bundle-SymbolicName"] = "org.mongodb.mongodb-driver-kotlin-extensions"
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs =
            listOf(
                // Adds OnlyInputTypes support
                "-Xallow-kotlin-package",
            )
    }
}
