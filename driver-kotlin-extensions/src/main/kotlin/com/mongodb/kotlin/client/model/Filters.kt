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
@file:Suppress("TooManyFunctions")

package com.mongodb.kotlin.client.model

import com.mongodb.client.model.Filters
import com.mongodb.client.model.TextSearchOptions
import com.mongodb.client.model.geojson.Geometry
import com.mongodb.client.model.geojson.Point
import java.util.regex.Pattern
import kotlin.internal.OnlyInputTypes
import kotlin.reflect.KProperty
import org.bson.BsonType
import org.bson.conversions.Bson

/**
 * Filters extension methods to improve Kotlin interop
 *
 * @since 5.3
 */
public object Filters {

    /**
     * Creates a filter that matches all documents where the value of the property equals the specified value. Note that
     * this doesn't actually generate an $eq operator, as the query language doesn't require it.
     *
     * @param value the value, which may be null
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("eqExt")
    public infix fun <@OnlyInputTypes T> KProperty<T?>.eq(value: T?): Bson = Filters.eq(path(), value)

    /**
     * Creates a filter that matches all documents where the value of the property equals the specified value. Note that
     * this doesn't actually generate an $eq operator, as the query language doesn't require it.
     *
     * @param property the data class property
     * @param value the value, which may be null
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> eq(property: KProperty<T?>, value: T?): Bson = property.eq(value)

    /**
     * Creates a filter that matches all documents where the value of the property does not equal the specified value.
     *
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("neExt")
    public infix fun <@OnlyInputTypes T> KProperty<T?>.ne(value: T?): Bson = Filters.ne(path(), value)

    /**
     * Creates a filter that matches all documents where the value of the property does not equal the specified value.
     *
     * @param property the data class property
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> ne(property: KProperty<T?>, value: T?): Bson = property.ne(value)

    /**
     * Creates a filter that matches all documents where the value of the given property is less than the specified
     * value.
     *
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("ltExt")
    public infix fun <@OnlyInputTypes T> KProperty<T?>.lt(value: T): Bson = Filters.lt(path(), value)

    /**
     * Creates a filter that matches all documents where the value of the given property is less than the specified
     * value.
     *
     * @param property the data class property
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> lt(property: KProperty<T?>, value: T): Bson = property.lt(value)

    /**
     * Creates a filter that matches all documents where the value of the given property is less than or equal to the
     * specified value.
     *
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("lteExt")
    public infix fun <@OnlyInputTypes T> KProperty<T?>.lte(value: T): Bson = Filters.lte(path(), value)

    /**
     * Creates a filter that matches all documents where the value of the given property is less than or equal to the
     * specified value.
     *
     * @param property the data class property
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> lte(property: KProperty<T?>, value: T): Bson = property.lte(value)

    /**
     * Creates a filter that matches all documents where the value of the given property is greater than the specified
     * value.
     *
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("gtExt")
    public infix fun <@OnlyInputTypes T> KProperty<T>.gt(value: T): Bson = Filters.gt(path(), value)

    /**
     * Creates a filter that matches all documents where the value of the given property is greater than the specified
     * value.
     *
     * @param property the data class property
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> gt(property: KProperty<T>, value: T): Bson = property.gt(value)

    /**
     * Creates a filter that matches all documents where the value of the given property is greater than or equal to the
     * specified value.
     *
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("gteExt")
    public infix fun <@OnlyInputTypes T> KProperty<T>.gte(value: T): Bson = Filters.gte(path(), value)

    /**
     * Creates a filter that matches all documents where the value of the given property is greater than or equal to the
     * specified value.
     *
     * @param property the data class property
     * @param value the value
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> gte(property: KProperty<T>, value: T): Bson = property.gte(value)

    /**
     * Creates a filter that matches all documents where the value of a property equals any value in the list of
     * specified values.
     *
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @Suppress("FunctionNaming")
    @JvmSynthetic
    @JvmName("inExt")
    public infix fun <@OnlyInputTypes T> KProperty<T?>.`in`(values: Iterable<T?>): Bson = Filters.`in`(path(), values)

    /**
     * Creates a filter that matches all documents where the value of a property equals any value in the list of
     * specified values.
     *
     * @param property the data class property
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @Suppress("FunctionNaming")
    public fun <@OnlyInputTypes T> `in`(property: KProperty<T?>, values: Iterable<T?>): Bson = property.`in`(values)

    /**
     * Creates a filter that matches all documents where the value of a property equals any value in the list of
     * specified values.
     *
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @Suppress("FunctionNaming")
    @JvmSynthetic
    @JvmName("inIterableExt")
    public infix fun <@OnlyInputTypes T> KProperty<Iterable<T>?>.`in`(values: Iterable<T?>): Bson =
        Filters.`in`(path(), values)

    /**
     * Creates a filter that matches all documents where the value of a property equals any value in the list of
     * specified values.
     *
     * @param property the data class property
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @Suppress("FunctionNaming")
    @JvmSynthetic
    @JvmName("inIterable")
    public fun <@OnlyInputTypes T> `in`(property: KProperty<Iterable<T>?>, values: Iterable<T?>): Bson =
        property.`in`(values)

    /**
     * Creates a filter that matches all documents where the value of a property does not equal any of the specified
     * values or does not exist.
     *
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("ninExt")
    public infix fun <@OnlyInputTypes T> KProperty<T?>.nin(values: Iterable<T?>): Bson = Filters.nin(path(), values)

    /**
     * Creates a filter that matches all documents where the value of a property does not equal any of the specified
     * values or does not exist.
     *
     * @param property the data class property
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> nin(property: KProperty<T?>, values: Iterable<T?>): Bson = property.nin(values)

    /**
     * Creates a filter that matches all documents where the value of a property does not equal any of the specified
     * values or does not exist.
     *
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("ninIterableExt")
    public infix fun <@OnlyInputTypes T> KProperty<Iterable<T>?>.nin(values: Iterable<T?>): Bson =
        Filters.nin(path(), values)

    /**
     * Creates a filter that matches all documents where the value of a property does not equal any of the specified
     * values or does not exist.
     *
     * @param property the data class property
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("ninIterable")
    public fun <@OnlyInputTypes T> nin(property: KProperty<Iterable<T>?>, values: Iterable<T?>): Bson =
        property.nin(values)

    /**
     * Creates a filter that performs a logical AND of the provided list of filters. Note that this will only generate
     * an "$and" operator if absolutely necessary, as the query language implicitly ands together all the keys. In other
     * words, a query expression like:
     * ```and(eq("x", 1), lt("y", 3))```
     *
     * will generate a MongoDB query like: `{x : 1, y : {$lt : 3}}``
     *
     * @param filters the list of filters to and together
     * @return the filter
     */
    public fun and(filters: Iterable<Bson?>): Bson = Filters.and(filters.filterNotNull())

    /**
     * Creates a filter that performs a logical AND of the provided list of filters. Note that this will only generate
     * an "$and" operator if absolutely necessary, as the query language implicitly ands together all the keys. In other
     * words, a query expression like:
     * ```and(eq("x", 1), lt("y", 3))```
     *
     * will generate a MongoDB query like: `{x : 1, y : {$lt : 3}}``
     *
     * @param filters the list of filters to and together
     * @return the filter
     */
    public fun and(vararg filters: Bson?): Bson = and(filters.toList())

    /**
     * Creates a filter that preforms a logical OR of the provided list of filters.
     *
     * @param filters the list of filters to and together
     * @return the filter
     */
    public fun or(filters: Iterable<Bson?>): Bson = Filters.or(filters.filterNotNull())

    /**
     * Creates a filter that preforms a logical OR of the provided list of filters.
     *
     * @param filters the list of filters to and together
     * @return the filter
     */
    public fun or(vararg filters: Bson?): Bson = or(filters.toList())

    /**
     * Creates a filter that matches all documents that do not match the passed in filter. Requires the property to
     * passed as part of the value passed in and lifts it to create a valid "$not" query:
     * ```not(eq("x", 1))```
     *
     * will generate a MongoDB query like: `{x : $not: {$eq : 1}}`
     *
     * @param filter the value
     * @return the filter
     */
    public fun not(filter: Bson): Bson = Filters.not(filter)

    /**
     * Creates a filter that performs a logical NOR operation on all the specified filters.
     *
     * @param filters the list of values
     * @return the filter
     */
    public fun nor(vararg filters: Bson): Bson = Filters.nor(*filters)

    /**
     * Creates a filter that performs a logical NOR operation on all the specified filters.
     *
     * @param filters the list of values
     * @return the filter
     */
    public fun nor(filters: Iterable<Bson>): Bson = Filters.nor(filters)

    /**
     * Creates a filter that matches all documents that contain the given property.
     *
     * @return the filter
     */
    @JvmSynthetic @JvmName("existsExt") public fun <T> KProperty<T?>.exists(): Bson = Filters.exists(path())

    /**
     * Creates a filter that matches all documents that contain the given property.
     *
     * @param property the data class property
     * @return the filter
     */
    public fun <T> exists(property: KProperty<T?>): Bson = Filters.exists(property.path())

    /**
     * Creates a filter that matches all documents that either contain or do not contain the given property, depending
     * on the value of the exists parameter.
     *
     * @param exists true to check for existence, false to check for absence
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("existsExt")
    public infix fun <T> KProperty<T?>.exists(exists: Boolean): Bson = Filters.exists(path(), exists)

    /**
     * Creates a filter that matches all documents that either contain or do not contain the given property, depending
     * on the value of the exists parameter.
     *
     * @param property the data class property
     * @param exists true to check for existence, false to check for absence
     * @return the filter
     */
    public fun <T> exists(property: KProperty<T?>, exists: Boolean): Bson = property.exists(exists)

    /**
     * Creates a filter that matches all documents where the value of the property is of the specified BSON type.
     *
     * @param type the BSON type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("typeExt")
    public infix fun <T> KProperty<T?>.type(type: BsonType): Bson = Filters.type(path(), type)

    /**
     * Creates a filter that matches all documents where the value of the property is of the specified BSON type.
     *
     * @param property the data class property
     * @param type the BSON type
     * @return the filter
     */
    public fun <T> type(property: KProperty<T?>, type: BsonType): Bson = property.type(type)

    /**
     * Creates a filter that matches all documents where the value of a property divided by a divisor has the specified
     * remainder (i.e. perform a modulo operation to select documents).
     *
     * @param divisor the modulus
     * @param remainder the remainder
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("modExt")
    public fun <T> KProperty<T?>.mod(divisor: Long, remainder: Long): Bson = Filters.mod(path(), divisor, remainder)

    /**
     * Creates a filter that matches all documents where the value of a property divided by a divisor has the specified
     * remainder (i.e. perform a modulo operation to select documents).
     *
     * @param property the data class property
     * @param divisor the modulus
     * @param remainder the remainder
     * @return the filter
     */
    public fun <T> mod(property: KProperty<T?>, divisor: Long, remainder: Long): Bson = property.mod(divisor, remainder)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param pattern the pattern
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexExt")
    public infix fun KProperty<String?>.regex(pattern: String): Bson = Filters.regex(path(), pattern)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param property the data class property
     * @param pattern the pattern
     * @return the filter
     */
    public fun regex(property: KProperty<String?>, pattern: String): Bson = property.regex(pattern)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param pattern the pattern
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexExt")
    public infix fun KProperty<String?>.regex(pattern: Pattern): Bson = Filters.regex(path(), pattern)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param property the data class property
     * @param pattern the pattern
     * @return the filter
     */
    public fun regex(property: KProperty<String?>, pattern: Pattern): Bson = property.regex(pattern)

    /**
     * Creates a filter that matches all documents where the value of the option matches the given regular expression
     * pattern with the given options applied.
     *
     * @param pattern the pattern
     * @param options the options
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexExt")
    public fun KProperty<String?>.regex(pattern: String, options: String): Bson =
        Filters.regex(path(), pattern, options)

    /**
     * Creates a filter that matches all documents where the value of the option matches the given regular expression
     * pattern with the given options applied.
     *
     * @param property the data class property
     * @param pattern the pattern
     * @param options the options
     * @return the filter
     */
    public fun regex(property: KProperty<String?>, pattern: String, options: String): Bson =
        property.regex(pattern, options)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param regex the regex
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexExt")
    public infix fun KProperty<String?>.regex(regex: Regex): Bson = Filters.regex(path(), regex.toPattern())

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param property the data class property
     * @param regex the regex
     * @return the filter
     */
    public fun regex(property: KProperty<String?>, regex: Regex): Bson = property.regex(regex.toPattern())

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param pattern the pattern
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexIterableExt")
    public infix fun KProperty<Iterable<String?>>.regex(pattern: String): Bson = Filters.regex(path(), pattern)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param property the data class property
     * @param pattern the pattern
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexIterable")
    public fun regex(property: KProperty<Iterable<String?>>, pattern: String): Bson = property.regex(pattern)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param pattern the pattern
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexIterableExt")
    public infix fun KProperty<Iterable<String?>>.regex(pattern: Pattern): Bson = Filters.regex(path(), pattern)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param property the data class property
     * @param pattern the pattern
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexIterable")
    public fun regex(property: KProperty<Iterable<String?>>, pattern: Pattern): Bson = property.regex(pattern)

    /**
     * Creates a filter that matches all documents where the value of the option matches the given regular expression
     * pattern with the given options applied.
     *
     * @param regex the regex pattern
     * @param options the options
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexIterableExt")
    public fun KProperty<Iterable<String?>>.regex(regex: String, options: String): Bson =
        Filters.regex(path(), regex, options)

    /**
     * Creates a filter that matches all documents where the value of the option matches the given regular expression
     * pattern with the given options applied.
     *
     * @param property the data class property
     * @param regex the regex pattern
     * @param options the options
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexIterable")
    public fun regex(property: KProperty<Iterable<String?>>, regex: String, options: String): Bson =
        property.regex(regex, options)

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param regex the regex
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexIterableExt")
    public infix fun KProperty<Iterable<String?>>.regex(regex: Regex): Bson = Filters.regex(path(), regex.toPattern())

    /**
     * Creates a filter that matches all documents where the value of the property matches the given regular expression
     * pattern.
     *
     * @param property the data class property
     * @param regex the regex
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("regexIterable")
    public fun regex(property: KProperty<Iterable<String?>>, regex: Regex): Bson = property.regex(regex.toPattern())

    /**
     * Creates a filter that matches all documents matching the given the search term with the given text search
     * options.
     *
     * @param search the search term
     * @param textSearchOptions the text search options to use
     * @return the filter
     */
    public fun text(search: String, textSearchOptions: TextSearchOptions = TextSearchOptions()): Bson =
        Filters.text(search, textSearchOptions)

    /**
     * Creates a filter that matches all documents for which the given expression is true.
     *
     * @param javaScriptExpression the JavaScript expression
     * @return the filter
     */
    public fun where(javaScriptExpression: String): Bson = Filters.where(javaScriptExpression)

    /**
     * Creates a filter that matches all documents that validate against the given JSON schema document.
     *
     * @param expression the aggregation expression
     * @param <T> the expression type
     * @return the filter
     */
    public fun <T> expr(expression: T): Bson = Filters.expr(expression)

    /**
     * Creates a filter that matches all documents where the value of a property is an array that contains all the
     * specified values.
     *
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("allExt")
    public infix fun <@OnlyInputTypes T> KProperty<Iterable<T>?>.all(values: Iterable<T>): Bson =
        Filters.all(path(), values)

    /**
     * Creates a filter that matches all documents where the value of a property is an array that contains all the
     * specified values.
     *
     * @param property the data class property
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> all(property: KProperty<Iterable<T>?>, values: Iterable<T>): Bson =
        property.all(values)

    /**
     * Creates a filter that matches all documents where the value of a property is an array that contains all the
     * specified values.
     *
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("allvargsExt")
    public fun <@OnlyInputTypes T> KProperty<Iterable<T>?>.all(vararg values: T): Bson = Filters.all(path(), *values)

    /**
     * Creates a filter that matches all documents where the value of a property is an array that contains all the
     * specified values.
     *
     * @param property the data class property
     * @param values the list of values
     * @param <T> the value type
     * @return the filter
     */
    public fun <@OnlyInputTypes T> all(property: KProperty<Iterable<T>?>, vararg values: T): Bson =
        property.all(*values)

    /**
     * Creates a filter that matches all documents containing a property that is an array where at least one member of
     * the array matches the given filter.
     *
     * @param filter the filter to apply to each element
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("elemMatchExt")
    public infix fun <T> KProperty<Iterable<T>?>.elemMatch(filter: Bson): Bson = Filters.elemMatch(path(), filter)

    /**
     * Creates a filter that matches all documents containing a property that is an array where at least one member of
     * the array matches the given filter.
     *
     * @param property the data class property
     * @param filter the filter to apply to each element
     * @return the filter
     */
    public fun <T> elemMatch(property: KProperty<Iterable<T>?>, filter: Bson): Bson = property.elemMatch(filter)

    /**
     * Creates a filter that matches all documents where the value of a property is an array of the specified size.
     *
     * @param size the size of the array
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("sizeExt")
    public infix fun <T> KProperty<T?>.size(size: Int): Bson = Filters.size(path(), size)

    /**
     * Creates a filter that matches all documents where the value of a property is an array of the specified size.
     *
     * @param property the data class property
     * @param size the size of the array
     * @return the filter
     */
    public fun <T> size(property: KProperty<T?>, size: Int): Bson = property.size(size)

    /**
     * Creates a filter that matches all documents where all of the bit positions are clear in the property.
     *
     * @param bitmask the bitmask
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("bitsAllClearExt")
    public infix fun <T> KProperty<T?>.bitsAllClear(bitmask: Long): Bson = Filters.bitsAllClear(path(), bitmask)

    /**
     * Creates a filter that matches all documents where all of the bit positions are clear in the property.
     *
     * @param property the data class property
     * @param bitmask the bitmask
     * @return the filter
     */
    public fun <T> bitsAllClear(property: KProperty<T?>, bitmask: Long): Bson = property.bitsAllClear(bitmask)

    /**
     * Creates a filter that matches all documents where all of the bit positions are set in the property.
     *
     * @param bitmask the bitmask
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("bitsAllSetExt")
    public infix fun <T> KProperty<T?>.bitsAllSet(bitmask: Long): Bson = Filters.bitsAllSet(path(), bitmask)

    /**
     * Creates a filter that matches all documents where all of the bit positions are set in the property.
     *
     * @param property the data class property
     * @param bitmask the bitmask
     * @return the filter
     */
    public fun <T> bitsAllSet(property: KProperty<T?>, bitmask: Long): Bson = property.bitsAllSet(bitmask)

    /**
     * Creates a filter that matches all documents where any of the bit positions are clear in the property.
     *
     * @param bitmask the bitmask
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("bitsAnyClearExt")
    public infix fun <T> KProperty<T?>.bitsAnyClear(bitmask: Long): Bson = Filters.bitsAnyClear(path(), bitmask)

    /**
     * Creates a filter that matches all documents where any of the bit positions are clear in the property.
     *
     * @param property the data class property
     * @param bitmask the bitmask
     * @return the filter
     */
    public fun <T> bitsAnyClear(property: KProperty<T?>, bitmask: Long): Bson = property.bitsAnyClear(bitmask)

    /**
     * Creates a filter that matches all documents where any of the bit positions are set in the property.
     *
     * @param bitmask the bitmask
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("bitsAnySetExt")
    public infix fun <T> KProperty<T?>.bitsAnySet(bitmask: Long): Bson = Filters.bitsAnySet(path(), bitmask)

    /**
     * Creates a filter that matches all documents where any of the bit positions are set in the property.
     *
     * @param property the data class property
     * @param bitmask the bitmask
     * @return the filter
     */
    public fun <T> bitsAnySet(property: KProperty<T?>, bitmask: Long): Bson = property.bitsAnySet(bitmask)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that exists entirely
     * within the specified shape.
     *
     * @param geometry the bounding GeoJSON geometry object
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("geoWithinExt")
    public infix fun <T> KProperty<T?>.geoWithin(geometry: Geometry): Bson = Filters.geoWithin(path(), geometry)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that exists entirely
     * within the specified shape.
     *
     * @param property the data class property
     * @param geometry the bounding GeoJSON geometry object
     * @return the filter
     */
    public fun <T> geoWithin(property: KProperty<T?>, geometry: Geometry): Bson = property.geoWithin(geometry)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that exists entirely
     * within the specified shape.
     *
     * @param geometry the bounding GeoJSON geometry object
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("geoWithinExt")
    public infix fun <T> KProperty<T?>.geoWithin(geometry: Bson): Bson = Filters.geoWithin(path(), geometry)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that exists entirely
     * within the specified shape.
     *
     * @param property the data class property
     * @param geometry the bounding GeoJSON geometry object
     * @return the filter
     */
    public fun <T> geoWithin(property: KProperty<T?>, geometry: Bson): Bson = property.geoWithin(geometry)

    /**
     * Creates a filter that matches all documents containing a property with grid coordinates data that exist entirely
     * within the specified box.
     *
     * @param lowerLeftX the lower left x coordinate of the box
     * @param lowerLeftY the lower left y coordinate of the box
     * @param upperRightX the upper left x coordinate of the box
     * @param upperRightY the upper left y coordinate of the box
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("geoWithinBoxExt")
    public fun <T> KProperty<T?>.geoWithinBox(
        lowerLeftX: Double,
        lowerLeftY: Double,
        upperRightX: Double,
        upperRightY: Double
    ): Bson = Filters.geoWithinBox(path(), lowerLeftX, lowerLeftY, upperRightX, upperRightY)

    /**
     * Creates a filter that matches all documents containing a property with grid coordinates data that exist entirely
     * within the specified box.
     *
     * @param property the data class property
     * @param lowerLeftX the lower left x coordinate of the box
     * @param lowerLeftY the lower left y coordinate of the box
     * @param upperRightX the upper left x coordinate of the box
     * @param upperRightY the upper left y coordinate of the box
     * @return the filter
     */
    public fun <T> geoWithinBox(
        property: KProperty<T?>,
        lowerLeftX: Double,
        lowerLeftY: Double,
        upperRightX: Double,
        upperRightY: Double
    ): Bson = property.geoWithinBox(lowerLeftX, lowerLeftY, upperRightX, upperRightY)

    /**
     * Creates a filter that matches all documents containing a property with grid coordinates data that exist entirely
     * within the specified polygon.
     *
     * @param points a list of pairs of x, y coordinates. Any extra dimensions are ignored
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("geoWithinPolygonExt")
    public infix fun <T> KProperty<T?>.geoWithinPolygon(points: List<List<Double>>): Bson =
        Filters.geoWithinPolygon(path(), points)

    /**
     * Creates a filter that matches all documents containing a property with grid coordinates data that exist entirely
     * within the specified polygon.
     *
     * @param property the data class property
     * @param points a list of pairs of x, y coordinates. Any extra dimensions are ignored
     * @return the filter
     */
    public fun <T> geoWithinPolygon(property: KProperty<T?>, points: List<List<Double>>): Bson =
        property.geoWithinPolygon(points)

    /**
     * Creates a filter that matches all documents containing a property with grid coordinates data that exist entirely
     * within the specified circle.
     *
     * @param x the x coordinate of the circle
     * @param y the y coordinate of the circle
     * @param radius the radius of the circle, as measured in the units used by the coordinate system
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("geoWithinCenterExt")
    public fun <T> KProperty<T?>.geoWithinCenter(x: Double, y: Double, radius: Double): Bson =
        Filters.geoWithinCenter(path(), x, y, radius)

    /**
     * Creates a filter that matches all documents containing a property with grid coordinates data that exist entirely
     * within the specified circle.
     *
     * @param property the data class property
     * @param x the x coordinate of the circle
     * @param y the y coordinate of the circle
     * @param radius the radius of the circle, as measured in the units used by the coordinate system
     * @return the filter
     */
    public fun <T> geoWithinCenter(property: KProperty<T?>, x: Double, y: Double, radius: Double): Bson =
        property.geoWithinCenter(x, y, radius)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data (GeoJSON or legacy
     * coordinate pairs) that exist entirely within the specified circle, using spherical geometry. If using longitude
     * and latitude, specify longitude first.
     *
     * @param x the x coordinate of the circle
     * @param y the y coordinate of the circle
     * @param radius the radius of the circle, in radians
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("geoWithinCenterSphereExt")
    public fun <T> KProperty<T?>.geoWithinCenterSphere(x: Double, y: Double, radius: Double): Bson =
        Filters.geoWithinCenterSphere(path(), x, y, radius)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data (GeoJSON or legacy
     * coordinate pairs) that exist entirely within the specified circle, using spherical geometry. If using longitude
     * and latitude, specify longitude first.
     *
     * @param property the data class property
     * @param x the x coordinate of the circle
     * @param y the y coordinate of the circle
     * @param radius the radius of the circle, in radians
     * @return the filter
     */
    public fun <T> geoWithinCenterSphere(property: KProperty<T?>, x: Double, y: Double, radius: Double): Bson =
        property.geoWithinCenterSphere(x, y, radius)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that intersects with the
     * specified shape.
     *
     * @param geometry the bounding GeoJSON geometry object
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("geoIntersectsExt")
    public infix fun <T> KProperty<T?>.geoIntersects(geometry: Geometry): Bson = Filters.geoIntersects(path(), geometry)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that intersects with the
     * specified shape.
     *
     * @param property the data class property
     * @param geometry the bounding GeoJSON geometry object
     * @return the filter
     */
    public fun <T> geoIntersects(property: KProperty<T?>, geometry: Geometry): Bson = property.geoIntersects(geometry)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that intersects with the
     * specified shape.
     *
     * @param geometry the bounding GeoJSON geometry object
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("geoIntersectsExt")
    public infix fun <T> KProperty<T?>.geoIntersects(geometry: Bson): Bson = Filters.geoIntersects(path(), geometry)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that intersects with the
     * specified shape.
     *
     * @param property the data class property
     * @param geometry the bounding GeoJSON geometry object
     * @return the filter
     */
    public fun <T> geoIntersects(property: KProperty<T?>, geometry: Bson): Bson = property.geoIntersects(geometry)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * GeoJSON point.
     *
     * @param geometry the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters
     * @param minDistance the minimum distance from the point, in meters
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("nearExt")
    public fun <T> KProperty<T?>.near(geometry: Point, maxDistance: Double? = null, minDistance: Double? = null): Bson =
        Filters.near(path(), geometry, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * GeoJSON point.
     *
     * @param property the data class property
     * @param geometry the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters
     * @param minDistance the minimum distance from the point, in meters
     * @return the filter
     */
    public fun <T> near(
        property: KProperty<T?>,
        geometry: Point,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = property.near(geometry, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * GeoJSON point.
     *
     * @param geometry the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters
     * @param minDistance the minimum distance from the point, in meters
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("nearExt")
    public fun <T> KProperty<T?>.near(geometry: Bson, maxDistance: Double? = null, minDistance: Double? = null): Bson =
        Filters.near(path(), geometry, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * GeoJSON point.
     *
     * @param property the data class property
     * @param geometry the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters
     * @param minDistance the minimum distance from the point, in meters
     * @return the filter
     */
    public fun <T> near(
        property: KProperty<T?>,
        geometry: Bson,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = property.near(geometry, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * point.
     *
     * @param x the x coordinate
     * @param y the y coordinate
     * @param maxDistance the maximum distance from the point, in radians
     * @param minDistance the minimum distance from the point, in radians
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("nearExt")
    public fun <T> KProperty<T?>.near(
        x: Double,
        y: Double,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = Filters.near(path(), x, y, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * point.
     *
     * @param property the data class property
     * @param x the x coordinate
     * @param y the y coordinate
     * @param maxDistance the maximum distance from the point, in radians
     * @param minDistance the minimum distance from the point, in radians
     * @return the filter
     */
    public fun <T> near(
        property: KProperty<T?>,
        x: Double,
        y: Double,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = property.near(x, y, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * GeoJSON point using spherical geometry.
     *
     * @param geometry the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters
     * @param minDistance the minimum distance from the point, in meters
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("nearSphereExt")
    public fun <T> KProperty<T?>.nearSphere(
        geometry: Bson,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = Filters.nearSphere(path(), geometry, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * GeoJSON point using spherical geometry.
     *
     * @param property the data class property
     * @param geometry the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters
     * @param minDistance the minimum distance from the point, in meters
     * @return the filter
     */
    public fun <T> nearSphere(
        property: KProperty<T?>,
        geometry: Bson,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = property.nearSphere(geometry, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * GeoJSON point using spherical geometry.
     *
     * @param geometry the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters
     * @param minDistance the minimum distance from the point, in meters
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("nearSphereExt")
    public fun <T> KProperty<T?>.nearSphere(
        geometry: Point,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = Filters.nearSphere(path(), geometry, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * GeoJSON point using spherical geometry.
     *
     * @param property the data class property
     * @param geometry the bounding GeoJSON geometry object
     * @param maxDistance the maximum distance from the point, in meters
     * @param minDistance the minimum distance from the point, in meters
     * @return the filter
     */
    public fun <T> nearSphere(
        property: KProperty<T?>,
        geometry: Point,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = property.nearSphere(geometry, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * point using spherical geometry.
     *
     * @param x the x coordinate
     * @param y the y coordinate
     * @param maxDistance the maximum distance from the point, in radians
     * @param minDistance the minimum distance from the point, in radians
     * @return the filter
     */
    @JvmSynthetic
    @JvmName("nearSphereExt")
    public fun <T> KProperty<T?>.nearSphere(
        x: Double,
        y: Double,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = Filters.nearSphere(path(), x, y, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents containing a property with geospatial data that is near the specified
     * point using spherical geometry.
     *
     * @param property the data class property
     * @param x the x coordinate
     * @param y the y coordinate
     * @param maxDistance the maximum distance from the point, in radians
     * @param minDistance the minimum distance from the point, in radians
     * @return the filter
     */
    public fun <T> nearSphere(
        property: KProperty<T?>,
        x: Double,
        y: Double,
        maxDistance: Double? = null,
        minDistance: Double? = null
    ): Bson = property.nearSphere(x, y, maxDistance, minDistance)

    /**
     * Creates a filter that matches all documents that validate against the given JSON schema document.
     *
     * @param schema the JSON schema to validate against
     * @return the filter
     */
    public fun jsonSchema(schema: Bson): Bson = Filters.jsonSchema(schema)
}
