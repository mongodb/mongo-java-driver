/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (C) 2016/2022 Litote
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
 *
 * @custom-license-header
 */
package com.mongodb.kotlin.client.model

import com.mongodb.client.model.Indexes
import kotlin.reflect.KProperty
import org.bson.conversions.Bson

/**
 * Indexes extension methods to improve Kotlin interop
 *
 * @since 5.3
 */
public object Indexes {
    /**
     * Create an index key for an ascending index on the given fields.
     *
     * @param properties the properties, which must contain at least one
     * @return the index specification @mongodb.driver.manual core/indexes indexes
     */
    public fun ascending(vararg properties: KProperty<*>): Bson = Indexes.ascending(properties.map { it.path() })

    /**
     * Create an index key for an ascending index on the given fields.
     *
     * @param properties the properties, which must contain at least one
     * @return the index specification @mongodb.driver.manual core/indexes indexes
     */
    public fun ascending(properties: Iterable<KProperty<*>>): Bson = Indexes.ascending(properties.map { it.path() })

    /**
     * Create an index key for a descending index on the given fields.
     *
     * @param properties the properties, which must contain at least one
     * @return the index specification @mongodb.driver.manual core/indexes indexes
     */
    public fun descending(vararg properties: KProperty<*>): Bson = Indexes.descending(properties.map { it.path() })

    /**
     * Create an index key for a descending index on the given fields.
     *
     * @param properties the properties, which must contain at least one
     * @return the index specification @mongodb.driver.manual core/indexes indexes
     */
    public fun descending(properties: Iterable<KProperty<*>>): Bson = Indexes.descending(properties.map { it.path() })

    /**
     * Create an index key for an 2dsphere index on the given fields.
     *
     * @param properties the properties, which must contain at least one
     * @return the index specification @mongodb.driver.manual core/2dsphere 2dsphere Index
     */
    public fun geo2dsphere(vararg properties: KProperty<*>): Bson = Indexes.geo2dsphere(properties.map { it.path() })

    /**
     * Create an index key for an 2dsphere index on the given fields.
     *
     * @param properties the properties, which must contain at least one
     * @return the index specification @mongodb.driver.manual core/2dsphere 2dsphere Index
     */
    public fun geo2dsphere(properties: Iterable<KProperty<*>>): Bson = Indexes.geo2dsphere(properties.map { it.path() })

    /**
     * Create an index key for a text index on the given property.
     *
     * @param property the property to create a text index on
     * @return the index specification @mongodb.driver.manual core/text text index
     */
    public fun <T> text(property: KProperty<T>): Bson = Indexes.text(property.path())

    /**
     * Create an index key for a hashed index on the given property.
     *
     * @param property the property to create a hashed index on
     * @return the index specification @mongodb.driver.manual core/hashed hashed index
     */
    public fun <T> hashed(property: KProperty<T>): Bson = Indexes.hashed(property.path())

    /**
     * Create an index key for a 2d index on the given field.
     *
     * <p>
     * <strong>Note: </strong>A 2d index is for data stored as points on a two-dimensional plane. The 2d index is
     * intended for legacy coordinate pairs used in MongoDB 2.2 and earlier. </p>
     *
     * @param property the property to create a 2d index on
     * @return the index specification @mongodb.driver.manual core/2d 2d index
     */
    public fun <T> geo2d(property: KProperty<T>): Bson = Indexes.geo2d(property.path())
}
