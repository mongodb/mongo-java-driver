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

package org.mongodb.kotlin.id.jvm

import org.mongodb.kotlin.id.Id
import org.mongodb.kotlin.id.IdGenerator
import org.mongodb.kotlin.id.IdGeneratorProvider
import org.mongodb.kotlin.id.StringId
import java.util.ServiceLoader
import kotlin.reflect.full.valueParameters

/**
 * Generate a new [Id] with the [IdGenerator.defaultGenerator].
 */
fun <T> newId(): Id<T> = loadIdGeneratorProvider().generator.generateNewId()

/**
 * Get the current [IdGeneratorProvider]
 */
fun loadIdGeneratorProvider(): IdGeneratorProvider =
    ServiceLoader.load(IdGeneratorProvider::class.java)
        .iterator().asSequence().maxByOrNull { it.priority } ?: UUIDStringIdGeneratorProvider()

/**
 * Create a new [Id] from the current [String].
 */
fun <T> String.toId(): Id<T> = StringId(this)

/**
 * Create a new id from its String representation.
 */
fun IdGenerator.createId(s: String): Id<*> = idClass
    .constructors
    .firstOrNull { it.valueParameters.size == 1 && it.valueParameters.first().type.classifier == String::class }
    ?.call(s)
    ?: error("no constructor with a single string arg found for $idClass}")

