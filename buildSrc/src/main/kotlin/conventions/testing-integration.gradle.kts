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

// Adds separate `integrationTest` configuration to a project
// Allows unit and integrations tests to be separate tasks
//
// See:
// https://docs.gradle.org/current/samples/sample_jvm_multi_project_with_additional_test_types.html
plugins { id("java-library") }

val integrationTest by sourceSets.creating

configurations[integrationTest.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())

configurations[integrationTest.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

val integrationTestTask =
    tasks.register<Test>("integrationTest") {
        description = "Runs integration tests."
        group = "verification"
        useJUnitPlatform()

        testClassesDirs = integrationTest.output.classesDirs
        classpath = configurations[integrationTest.runtimeClasspathConfigurationName] + integrationTest.output
        shouldRunAfter(tasks.test)
    }

tasks.findByName("check")?.dependsOn(integrationTestTask)

dependencies {
    "integrationTestImplementation"(project)
    "integrationTestImplementation"(platform(libs.junit.bom))
    "integrationTestImplementation"(libs.bundles.junit.vintage)
}

sourceSets["integrationTest"].java.srcDirs("src/integrationTest", "src/integrationTest/java")
