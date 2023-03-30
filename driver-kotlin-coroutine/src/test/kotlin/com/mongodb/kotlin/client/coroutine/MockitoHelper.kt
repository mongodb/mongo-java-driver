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
package com.mongodb.kotlin.client.coroutine

import org.assertj.core.api.Assertions.assertThat
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.argThat

/** Mockito test helper object */
object MockitoHelper {

    /**
     * Deep reflection comparison for complex nested objects
     *
     * The usecase is to reflect complex objects that don't have an equals method and contain nested complex properties
     * that also do not contain equals values
     *
     * Example:
     * ```
     *  verify(wrapped).createCollection(eq(name), deepRefEq(defaultOptions))
     * ```
     *
     * @param T the type of the value
     * @param value the value
     * @return the value
     * @see [org.mockito.kotlin.refEq]
     */
    fun <T> deepRefEq(value: T): T = argThat(DeepReflectionEqMatcher(value))

    private class DeepReflectionEqMatcher<T>(private val expected: T) : ArgumentMatcher<T> {
        override fun matches(argument: T): Boolean {
            return try {
                assertThat(argument).usingRecursiveComparison().isEqualTo(expected)
                true
            } catch (e: Throwable) {
                false
            }
        }
    }
}
