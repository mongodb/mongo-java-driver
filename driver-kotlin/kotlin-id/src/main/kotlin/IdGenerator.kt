/*
 *
 *  * Copyright 2008-present MongoDB, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.mongodb.kotlin.id

import java.util.ServiceLoader
import kotlin.reflect.KClass
import kotlin.reflect.full.valueParameters

/**
 * A generator of Ids.
 */
interface IdGenerator {

    companion object {

        var defaultGenerator: IdGenerator
            get() = defaultIdGenerator
            set(value) {
                defaultIdGenerator = value
            }

        @Volatile
        private var defaultIdGenerator: IdGenerator =
            ServiceLoader.load(IdGeneratorProvider::class.java)
                .iterator().asSequence().maxByOrNull { it.priority }?.generator
                    ?: UUIDStringIdGenerator
    }

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
    fun create(s: String): Id<*> = idClass
        .constructors
        .firstOrNull { it.valueParameters.size == 1 && it.valueParameters.first().type.classifier == String::class }
        ?.call(s)
            ?: error("no constructor with a single string arg found for $idClass}")

}
