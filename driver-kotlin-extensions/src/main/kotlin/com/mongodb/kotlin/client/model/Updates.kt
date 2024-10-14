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

import com.mongodb.client.model.PushOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import kotlin.internal.OnlyInputTypes
import kotlin.reflect.KProperty
import org.bson.conversions.Bson

/**
 * Updates extension methods to improve Kotlin interop
 *
 * @since 5.3
 */
@Suppress("TooManyFunctions")
public object Updates {

    /**
     * Creates an update that sets the value of the property to the given value.
     *
     * @param value the value
     * @param <T> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/set/ $set
     */
    @JvmSynthetic
    @JvmName("setExt")
    public infix fun <@OnlyInputTypes T> KProperty<T>.set(value: T?): Bson = Updates.set(path(), value)

    /**
     * Creates an update that sets the value of the property to the given value.
     *
     * @param property the data class property
     * @param value the value
     * @param <T> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/set/ $set
     */
    public fun <@OnlyInputTypes T> set(property: KProperty<T?>, value: T?): Bson = property.set(value)

    /**
     * Combine a list of updates into a single update.
     *
     * @param updates the list of updates
     * @return a combined update
     */
    public fun combine(vararg updates: Bson): Bson = Updates.combine(*updates)

    /**
     * Combine a list of updates into a single update.
     *
     * @param updates the list of updates
     * @return a combined update
     */
    public fun combine(updates: List<Bson>): Bson = Updates.combine(updates)

    /**
     * Creates an update that deletes the property with the given name.
     *
     * @param property the property
     * @return the update @mongodb.driver.manual reference/operator/update/unset/ $unset
     */
    public fun <T> unset(property: KProperty<T>): Bson = Updates.unset(property.path())

    /**
     * Creates an update that sets the value of the property to the given value, but only if the update is an upsert
     * that results in an insert of a document.
     *
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/setOnInsert/ $setOnInsert
     * @see UpdateOptions#upsert(boolean)
     */
    @JvmSynthetic
    @JvmName("setOnInsertExt")
    public infix fun <@OnlyInputTypes T> KProperty<T?>.setOnInsert(value: T?): Bson = Updates.setOnInsert(path(), value)

    /**
     * Creates an update that sets the value of the property to the given value, but only if the update is an upsert
     * that results in an insert of a document.
     *
     * @param property the property
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/setOnInsert/ $setOnInsert
     * @see UpdateOptions#upsert(boolean)
     */
    public fun <@OnlyInputTypes T> setOnInsert(property: KProperty<T?>, value: T?): Bson = property.setOnInsert(value)

    /**
     * Creates an update that renames a field.
     *
     * @param newProperty the new property
     * @return the update @mongodb.driver.manual reference/operator/update/rename/ $rename
     */
    @JvmSynthetic
    @JvmName("renameExt")
    public infix fun <@OnlyInputTypes T> KProperty<T?>.rename(newProperty: KProperty<T?>): Bson =
        Updates.rename(path(), newProperty.path())

    /**
     * Creates an update that renames a field.
     *
     * @param property the property
     * @param newProperty the new property
     * @return the update @mongodb.driver.manual reference/operator/update/rename/ $rename
     */
    public fun <@OnlyInputTypes T> rename(property: KProperty<T?>, newProperty: KProperty<T?>): Bson =
        property.rename(newProperty)

    /**
     * Creates an update that increments the value of the property by the given value.
     *
     * @param number the value
     * @return the update @mongodb.driver.manual reference/operator/update/inc/ $inc
     */
    @JvmSynthetic
    @JvmName("incExt")
    public infix fun <T : Number?> KProperty<T>.inc(number: Number): Bson = Updates.inc(path(), number)

    /**
     * Creates an update that increments the value of the property by the given value.
     *
     * @param property the property
     * @param number the value
     * @return the update @mongodb.driver.manual reference/operator/update/inc/ $inc
     */
    public fun <T : Number?> inc(property: KProperty<T>, number: Number): Bson = property.inc(number)

    /**
     * Creates an update that multiplies the value of the property by the given number.
     *
     * @param number the non-null number
     * @return the update @mongodb.driver.manual reference/operator/update/mul/ $mul
     */
    @JvmSynthetic
    @JvmName("mulExt")
    public infix fun <T : Number?> KProperty<T>.mul(number: Number): Bson = Updates.mul(path(), number)

    /**
     * Creates an update that multiplies the value of the property by the given number.
     *
     * @param property the property
     * @param number the non-null number
     * @return the update @mongodb.driver.manual reference/operator/update/mul/ $mul
     */
    public fun <T : Number?> mul(property: KProperty<T>, number: Number): Bson = property.mul(number)

    /**
     * Creates an update that sets the value of the property if the given value is less than the current value of the
     * property.
     *
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/min/ $min
     */
    @JvmSynthetic
    @JvmName("minExt")
    public infix fun <@OnlyInputTypes T> KProperty<T>.min(value: T): Bson = Updates.min(path(), value)

    /**
     * Creates an update that sets the value of the property if the given value is less than the current value of the
     * property.
     *
     * @param property the property
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/min/ $min
     */
    public fun <@OnlyInputTypes T> min(property: KProperty<T>, value: T): Bson = property.min(value)

    /**
     * Creates an update that sets the value of the property if the given value is greater than the current value of the
     * property.
     *
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/min/ $min
     */
    @JvmSynthetic
    @JvmName("maxExt")
    public infix fun <@OnlyInputTypes T> KProperty<T>.max(value: T): Bson = Updates.max(path(), value)

    /**
     * Creates an update that sets the value of the property if the given value is greater than the current value of the
     * property.
     *
     * @param property the property
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/min/ $min
     */
    public fun <@OnlyInputTypes T> max(property: KProperty<T>, value: T): Bson = property.max(value)

    /**
     * Creates an update that sets the value of the property to the current date as a BSON date.
     *
     * @param property the property
     * @return the update @mongodb.driver.manual reference/operator/update/currentDate/
     *   $currentDate @mongodb.driver.manual reference/bson-types/#date Date
     */
    public fun <T> currentDate(property: KProperty<T>): Bson = Updates.currentDate(property.path())

    /**
     * Creates an update that sets the value of the property to the current date as a BSON timestamp.
     *
     * @param property the property
     * @return the update @mongodb.driver.manual reference/operator/update/currentDate/
     *   $currentDate @mongodb.driver.manual reference/bson-types/#document-bson-type-timestamp Timestamp
     */
    public fun <T> currentTimestamp(property: KProperty<T>): Bson = Updates.currentTimestamp(property.path())

    /**
     * Creates an update that adds the given value to the array value of the property, unless the value is already
     * present, in which case it does nothing
     *
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/addToSet/ $addToSet
     */
    @JvmSynthetic
    @JvmName("addToSetExt")
    public infix fun <@OnlyInputTypes T> KProperty<Iterable<T>?>.addToSet(value: T): Bson =
        Updates.addToSet(path(), value)

    /**
     * Creates an update that adds the given value to the array value of the property, unless the value is already
     * present, in which case it does nothing
     *
     * @param property the property
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/addToSet/ $addToSet
     */
    public fun <@OnlyInputTypes T> addToSet(property: KProperty<Iterable<T>?>, value: T): Bson =
        property.addToSet(value)

    /**
     * Creates an update that adds each of the given values to the array value of the property, unless the value is
     * already present, in which case it does nothing
     *
     * @param property the property
     * @param values the values
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/addToSet/ $addToSet
     */
    public fun <@OnlyInputTypes T> addEachToSet(property: KProperty<Iterable<T>?>, values: List<T>): Bson =
        Updates.addEachToSet(property.path(), values)

    /**
     * Creates an update that adds the given value to the array value of the property.
     *
     * @param property the property
     * @param value the value
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/push/ $push
     */
    public fun <@OnlyInputTypes T> push(property: KProperty<Iterable<T>?>, value: T): Bson =
        Updates.push(property.path(), value)

    /**
     * Creates an update that adds each of the given values to the array value of the property, applying the given
     * options for positioning the pushed values, and then slicing and/or sorting the array.
     *
     * @param property the property
     * @param values the values
     * @param options the non-null push options
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/push/ $push
     */
    public fun <@OnlyInputTypes T> pushEach(
        property: KProperty<Iterable<T>?>,
        values: List<T?>,
        options: PushOptions = PushOptions()
    ): Bson = Updates.pushEach(property.path(), values, options)

    /**
     * Creates an update that removes all instances of the given value from the array value of the property.
     *
     * @param value the value
     * @param <T> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    @JvmSynthetic
    @JvmName("pullExt")
    public infix fun <@OnlyInputTypes T> KProperty<Iterable<T?>?>.pull(value: T?): Bson = Updates.pull(path(), value)

    /**
     * Creates an update that removes all instances of the given value from the array value of the property.
     *
     * @param property the property
     * @param value the value
     * @param <T> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    public fun <@OnlyInputTypes T> pull(property: KProperty<Iterable<T?>?>, value: T?): Bson = property.pull(value)

    /**
     * Creates an update that removes all instances of the given value from the array value of the property.
     *
     * @param filter the value
     * @return the update @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    @JvmSynthetic
    @JvmName("pullByFilterExt")
    public infix fun KProperty<*>.pullByFilter(filter: Bson): Bson = Updates.pull(path(), filter)

    /**
     * Creates an update that removes all instances of the given value from the array value of the property.
     *
     * @param property the property
     * @param filter the value
     * @return the update @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    public fun pullByFilter(property: KProperty<*>, filter: Bson): Bson = property.pullByFilter(filter)

    /**
     * Creates an update that removes from an array all elements that match the given filter.
     *
     * @param filter the query filter
     * @return the update @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    public fun pullByFilter(filter: Bson): Bson = Updates.pullByFilter(filter)

    /**
     * Creates an update that removes all instances of the given values from the array value of the property.
     *
     * @param values the values
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    @JvmSynthetic
    @JvmName("pullAllExt")
    public infix fun <@OnlyInputTypes T> KProperty<Iterable<T>?>.pullAll(values: List<T?>?): Bson =
        Updates.pullAll(path(), values ?: emptyList())

    /**
     * Creates an update that removes all instances of the given values from the array value of the property.
     *
     * @param property the property
     * @param values the values
     * @param <TItem> the value type
     * @return the update @mongodb.driver.manual reference/operator/update/pull/ $pull
     */
    public fun <@OnlyInputTypes T> pullAll(property: KProperty<Iterable<T>?>, values: List<T?>?): Bson =
        property.pullAll(values ?: emptyList())

    /**
     * Creates an update that pops the first element of an array that is the value of the property.
     *
     * @param property the property
     * @return the update @mongodb.driver.manual reference/operator/update/pop/ $pop
     */
    public fun <T> popFirst(property: KProperty<T>): Bson = Updates.popFirst(property.path())

    /**
     * Creates an update that pops the last element of an array that is the value of the property.
     *
     * @param property the property
     * @return the update @mongodb.driver.manual reference/operator/update/pop/ $pop
     */
    public fun <T> popLast(property: KProperty<T>): Bson = Updates.popLast(property.path())

    /**
     * Creates an update that performs a bitwise and between the given integer value and the integral value of the
     * property.
     *
     * @param value the value
     * @return the update
     */
    @JvmSynthetic
    @JvmName("bitwiseAndExt")
    public infix fun <T : Number?> KProperty<T>.bitwiseAnd(value: Int): Bson = Updates.bitwiseAnd(path(), value)

    /**
     * Creates an update that performs a bitwise and between the given integer value and the integral value of the
     * property.
     *
     * @param property the property
     * @param value the value
     * @return the update
     */
    public fun <T : Number?> bitwiseAnd(property: KProperty<T>, value: Int): Bson = property.bitwiseAnd(value)

    /**
     * Creates an update that performs a bitwise and between the given long value and the integral value of the
     * property.
     *
     * @param value the value
     * @return the update @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    @JvmSynthetic
    @JvmName("bitwiseAndExt")
    public infix fun <T : Number?> KProperty<T>.bitwiseAnd(value: Long): Bson = Updates.bitwiseAnd(path(), value)

    /**
     * Creates an update that performs a bitwise and between the given long value and the integral value of the
     * property.
     *
     * @param property the property
     * @param value the value
     * @return the update @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    public fun <T : Number?> bitwiseAnd(property: KProperty<T>, value: Long): Bson = property.bitwiseAnd(value)

    /**
     * Creates an update that performs a bitwise or between the given integer value and the integral value of the
     * property.
     *
     * @param value the value
     * @return the update @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    @JvmSynthetic
    @JvmName("bitwiseOrExt")
    public infix fun <T : Number?> KProperty<T>.bitwiseOr(value: Int): Bson = Updates.bitwiseOr(path(), value)

    /**
     * Creates an update that performs a bitwise or between the given integer value and the integral value of the
     * property.
     *
     * @param property the property
     * @param value the value
     * @return the update @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    public fun <T : Number?> bitwiseOr(property: KProperty<T>, value: Int): Bson =
        Updates.bitwiseOr(property.path(), value)

    /**
     * Creates an update that performs a bitwise or between the given long value and the integral value of the property.
     *
     * @param value the value
     * @return the update @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    @JvmSynthetic
    @JvmName("bitwiseOrExt")
    public infix fun <T : Number?> KProperty<T>.bitwiseOr(value: Long): Bson = Updates.bitwiseOr(path(), value)

    /**
     * Creates an update that performs a bitwise or between the given long value and the integral value of the property.
     *
     * @param property the property
     * @param value the value
     * @return the update @mongodb.driver.manual reference/operator/update/bit/ $bit
     */
    public fun <T : Number?> bitwiseOr(property: KProperty<T>, value: Long): Bson = property.bitwiseOr(value)

    /**
     * Creates an update that performs a bitwise xor between the given integer value and the integral value of the
     * property.
     *
     * @param value the value
     * @return the update
     */
    @JvmSynthetic
    @JvmName("bitwiseXorExt")
    public infix fun <T : Number?> KProperty<T>.bitwiseXor(value: Int): Bson = Updates.bitwiseXor(path(), value)

    /**
     * Creates an update that performs a bitwise xor between the given integer value and the integral value of the
     * property.
     *
     * @param property the property
     * @param value the value
     * @return the update
     */
    public fun <T : Number?> bitwiseXor(property: KProperty<T>, value: Int): Bson =
        Updates.bitwiseXor(property.path(), value)

    /**
     * Creates an update that performs a bitwise xor between the given long value and the integral value of the
     * property.
     *
     * @param value the value
     * @return the update
     */
    @JvmSynthetic
    @JvmName("addToSetExt")
    public infix fun <T : Number?> KProperty<T>.bitwiseXor(value: Long): Bson = Updates.bitwiseXor(path(), value)

    /**
     * Creates an update that performs a bitwise xor between the given long value and the integral value of the
     * property.
     *
     * @param property the property
     * @param value the value
     * @return the update
     */
    public fun <T : Number?> bitwiseXor(property: KProperty<T>, value: Long): Bson =
        Updates.bitwiseXor(property.path(), value)
}
