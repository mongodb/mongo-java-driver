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

import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.withType
import project.libs

// Adds groovy spock testing framework support
// See: https://spockframework.org/
plugins {
    id("conventions.testing-spock")
}

tasks.withType<Test>().configureEach {
    exclude("examples/**")
    useJUnitPlatform { excludeTags("Slow") }

    systemProperty("spock.configuration", "${rootProject.file("config/spock/ExcludeSlow.groovy")}")
}

tasks.register("testSlowOnly", Test::class.java) {
    useJUnitPlatform { includeTags("Slow") }

    systemProperty("spock.configuration", "${rootProject.file("config/spock/OnlySlow.groovy")}")
}
