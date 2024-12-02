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

import com.mongodb.client.model.Sorts
import kotlin.reflect.KProperty
import org.bson.conversions.Bson

/**
 * Sorts extension methods to improve Kotlin interop
 *
 * @since 5.3
 */
public object Sorts {

    /**
     * Create a sort specification for an ascending sort on the given properties.
     *
     * @param properties the properties, which must contain at least one
     * @return the sort specification @mongodb.driver.manual reference/operator/meta/orderby Sort
     */
    public fun ascending(vararg properties: KProperty<*>): Bson = ascending(properties.asList())

    /**
     * Create a sort specification for an ascending sort on the given properties.
     *
     * @param properties the properties, which must contain at least one
     * @return the sort specification @mongodb.driver.manual reference/operator/meta/orderby Sort
     */
    public fun ascending(properties: List<KProperty<*>>): Bson = Sorts.ascending(properties.map { it.path() })

    /**
     * Create a sort specification for a descending sort on the given properties.
     *
     * @param properties the properties, which must contain at least one
     * @return the sort specification @mongodb.driver.manual reference/operator/meta/orderby Sort
     */
    public fun descending(vararg properties: KProperty<*>): Bson = descending(properties.asList())

    /**
     * Create a sort specification for a descending sort on the given properties.
     *
     * @param properties the properties, which must contain at least one
     * @return the sort specification @mongodb.driver.manual reference/operator/meta/orderby Sort
     */
    public fun descending(properties: List<KProperty<*>>): Bson = Sorts.descending(properties.map { it.path() })

    /**
     * Create a sort specification for the text score meta projection on the given property.
     *
     * @param property the data class property
     * @return the sort specification @mongodb.driver.manual reference/operator/getProjection/meta/#sort textScore
     */
    public fun <T> metaTextScore(property: KProperty<T>): Bson = Sorts.metaTextScore(property.path())
}
