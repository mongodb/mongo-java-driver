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

    private fun getKotlinExtensions(className: String): Set<String> {
        // In the JVM extension methods in Filters.kt will become a class FiltersKt
        return ClassGraph()
            .enableClassInfo()
            .enableMethodInfo()
            .acceptPackages("com.mongodb.kotlin.client.model")
            .scan()
            .use {
                it.allClasses
                    .filter { it.simpleName == "${className}Kt" }
                    .flatMap { it.methodInfo }
                    .filter { it.isPublic }
                    .map { it.name }
                    .filter { !it.contains("$") }
                    .toSet()
            }
    }

    private fun getJavaMethods(className: String): Set<String> {
        // In the JVM extension methods in Filters.kt will become a class FiltersKt
        return ClassGraph().enableClassInfo().enableMethodInfo().acceptPackages("com.mongodb.client.model").scan().use {
            it.getClassInfo("com.mongodb.client.model.$className")
                .methodInfo
                .filter {
                    it.isPublic &&
                        it.parameterInfo.isNotEmpty() &&
                        it.parameterInfo[0].typeDescriptor.toStringWithSimpleNames().equals("String")
                }
                .map { m -> m.name }
                .toSet()
        }
    }
}
