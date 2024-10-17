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
package com.mongodb.kotlin.client.model

import io.github.classgraph.ClassGraph
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class ExtensionsApiTest {

    @Test
    fun shouldHaveAllFiltersExtensions() {
        val kotlinExtensions: Set<String> = getKotlinExtensions("Filters")
        val javaMethods: Set<String> = getJavaMethods("Filters")

        val notImplemented = javaMethods subtract kotlinExtensions
        assertTrue(notImplemented.isEmpty(), "Some possible Filters were not implemented: $notImplemented")
    }

    @Test
    fun shouldHaveAllProjectionsExtensions() {
        val kotlinExtensions: Set<String> = getKotlinExtensions("Projections")
        val javaMethods: Set<String> = getJavaMethods("Projections")

        val notImplemented = javaMethods subtract kotlinExtensions
        assertTrue(notImplemented.isEmpty(), "Some possible Projections were not implemented: $notImplemented")
    }

    @Test
    fun shouldHaveAllUpdatesExtensions() {
        val kotlinExtensions: Set<String> = getKotlinExtensions("Updates")
        val javaMethods: Set<String> = getJavaMethods("Updates")

        val notImplemented = javaMethods subtract kotlinExtensions
        assertTrue(notImplemented.isEmpty(), "Some possible Updates were not implemented: $notImplemented")
    }

    @Test
    fun shouldHaveAllIndexesExtensions() {
        val kotlinExtensions: Set<String> = getKotlinExtensions("Indexes")
        val javaMethods: Set<String> = getJavaMethods("Indexes")
        val notImplemented = javaMethods subtract kotlinExtensions
        assertTrue(notImplemented.isEmpty(), "Some possible Indexes were not implemented: $notImplemented")
    }

    private fun getKotlinExtensions(className: String): Set<String> {
        return ClassGraph()
            .enableClassInfo()
            .enableMethodInfo()
            .acceptPackages("com.mongodb.kotlin.client.model")
            .scan()
            .use { result ->
                result.allClasses
                    .filter { it.simpleName == className }
                    .asSequence()
                    .flatMap { it.methodInfo }
                    .filter { it.isPublic }
                    .map { it.name }
                    .filter { !it.contains("$") }
                    .toSet()
            }
    }

    private fun getJavaMethods(className: String): Set<String> {
        return ClassGraph().enableClassInfo().enableMethodInfo().acceptPackages("com.mongodb.client.model").scan().use {
            it.getClassInfo("com.mongodb.client.model.$className")
                .methodInfo
                .filter { methodInfo ->
                    methodInfo.isPublic &&
                        methodInfo.parameterInfo.isNotEmpty() &&
                        methodInfo.parameterInfo[0]
                            .typeDescriptor
                            .toStringWithSimpleNames()
                            .equals("String") // only method starting
                    // with a String (property name)
                }
                .map { m -> m.name }
                .toSet()
        }
    }
}
