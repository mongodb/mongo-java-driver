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

import com.diffplug.gradle.spotless.SpotlessApply
import com.diffplug.gradle.spotless.SpotlessCheck

plugins { id("com.diffplug.spotless") }

spotless {
    scala { scalafmt().configFile(rootProject.file("config/scala/scalafmt.conf")) }

    kotlinGradle {
        ktfmt("0.39").dropboxStyle().configure { it.setMaxWidth(120) }
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
        licenseHeaderFile(rootProject.file("config/mongodb.license"), "(group|plugins|import|buildscript|rootProject)")
    }

    kotlin {
        target("**/*.kt")
        ktfmt().dropboxStyle().configure { it.setMaxWidth(120) }
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
        licenseHeaderFile(rootProject.file("config/mongodb.license"))
    }

    format("extraneous") {
        target("*.xml", "*.yml", "*.md")
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
    }
}

tasks {
    withType<SpotlessApply>().configureEach {
        notCompatibleWithConfigurationCache("https://github.com/diffplug/spotless/issues/644")
    }
    withType<SpotlessCheck>().configureEach {
        notCompatibleWithConfigurationCache("https://github.com/diffplug/spotless/issues/644")
    }
}
