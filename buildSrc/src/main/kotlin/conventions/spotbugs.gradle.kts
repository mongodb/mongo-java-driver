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
import libs
import org.gradle.kotlin.dsl.dependencies

// Performs quality checks on your project's Java source files using SpotBug
// https://plugins.gradle.org/plugin/com.github.spotbugs
plugins {
    id("java-library")
    alias(libs.plugins.spotbugs)
}

dependencies {
    compileOnly(libs.findbugs.jsr)

    testImplementation(libs.findbugs.jsr)
}

spotbugs {
    if (!project.buildingWith("ssdlcReport.enabled")) {
        excludeFilter.set(rootProject.file("config/spotbugs/exclude.xml"))
    }
}

tasks.withType<SpotBugsTask>().configureEach {
    if (name == "spotbugsMain") {
        reports.create("xml") {
            required.set(project.buildingWith("xmlReports.enabled"))
            outputLocation.set(file("$buildDir/reports/spotbugs/spotbugs.xml"))
        }
        reports.create("html") {
            required.set(!project.buildingWith("xmlReports.enabled"))
            outputLocation.set(file("$buildDir/reports/spotbugs/spotbugs.html"))
        }
        reports.create("sarif") {
            required.set(project.buildingWith("ssdlcReport.enabled"))
            outputLocation.set(file("$buildDir/reports/spotbugs/spotbugs.sarif"))
        }
    } else if (name == "spotbugsTest") {
        enabled = false
    } else if (name == "spotbugsIntegrationTest") {
        enabled = false
    }
}
