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

import project.DEFAULT_JAVA_VERSION

// Allows test artifacts (resources and code) to be shared between projects
plugins { id("java-library") }

/** Create a test artifact configuration so that test resources can be consumed by other projects. */
val testArtifacts by configurations.creating
val testJar by
    tasks.registering(Jar::class) {
        archiveBaseName.set("${project.name}-test")
        from(sourceSets.test.get().output)
        setDuplicatesStrategy(DuplicatesStrategy.EXCLUDE)
    }

val testJavaVersion: Int = findProperty("javaVersion")?.toString()?.toInt() ?: DEFAULT_JAVA_VERSION

tasks.withType<Test>() {
    mustRunAfter(testJar)

    // Needed for OidcAuthenticationProseTests calls `field.setAccessible(true)`
    if (testJavaVersion >= DEFAULT_JAVA_VERSION) {
        jvmArgs("--add-opens=java.base/java.lang=ALL-UNNAMED")
    }
}

artifacts { add("testArtifacts", testJar) }
