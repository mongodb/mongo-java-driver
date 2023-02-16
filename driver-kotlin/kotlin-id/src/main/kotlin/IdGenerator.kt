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

package org.mongodb.kotlin.id

import kotlin.reflect.KClass

/**
 * Generator of [Id]s.
 */
interface IdGenerator {

    /**
     * The class of the id.
     */
    val idClass: KClass<out Id<*>>

    /**
     * The class of the wrapped id.
     */
    val wrappedIdClass: KClass<out Any>

    /**
     * Generate a new id.
     */
    fun <T> generateNewId(): Id<T>

    /**
     * Create a new id from its String representation.
     */
    fun create(s: String): Id<*>
}
