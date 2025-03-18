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

import com.github.spotbugs.snom.SpotBugsTask
import org.gradle.kotlin.dsl.dependencies
import project.libs

// Performs quality checks on your project's Java source files using SpotBug
// https://plugins.gradle.org/plugin/com.github.spotbugs
plugins {
    id("java-library")
    id("com.github.spotbugs")
}

dependencies {
    compileOnly(libs.findbugs.jsr)

    testImplementation(libs.findbugs.jsr)
}

spotbugs {
    if (!project.buildingWith("ssdlcReport.enabled")) {
        excludeFilter.set(rootProject.file("/config/spotbugs/exclude.xml"))
    }
}

tasks.withType<SpotBugsTask>().configureEach {
    if (name == "spotbugsMain") {
        reports {
            register("xml") { required.set(project.buildingWith("xmlReports.enabled")) }
            register("html") { required.set(!project.buildingWith("xmlReports.enabled")) }
            register("sarif") { required.set(project.buildingWith("ssdlcReport.enabled")) }
        }
    } else if (name == "spotbugsTest") {
        enabled = false
    } else if (name == "spotbugsIntegrationTest") {
        enabled = false
    }
}
