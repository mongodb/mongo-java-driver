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
package org.bson.codecs.kotlinx

/**
 * Bson Configuration for serialization
 *
 * Usage example with codecs:
 * ```
 * val codec = KotlinSerializerCodec.create(mykClass, bsonConfiguration = BsonConfiguration(encodeDefaults = false))
 * ```
 *
 * @property encodeDefaults encode default values, defaults to true
 * @property explicitNulls encode null values, defaults to false
 * @property classDiscriminator class discriminator to use when encoding polymorphic types
 */
public data class BsonConfiguration(
    val encodeDefaults: Boolean = true,
    val explicitNulls: Boolean = false,
    val classDiscriminator: String = "_t",
    val bsonNamingStrategy: BsonNamingStrategy? = null
)

/**
 * Optional BSON naming strategy for a field.
 *
 * @since 5.4
 */
public enum class BsonNamingStrategy {

    /**
     * A strategy that transforms serial names from camel case to snake case — lowercase characters with words separated
     * by underscores.
     */
    SNAKE_CASE,
}
