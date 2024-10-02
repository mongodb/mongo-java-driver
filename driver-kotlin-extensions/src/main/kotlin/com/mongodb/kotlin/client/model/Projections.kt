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

import com.mongodb.annotations.Beta
import com.mongodb.annotations.Reason
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.Projections
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1
import org.bson.conversions.Bson

/** The projection of the property. This is used in an aggregation pipeline to reference a property from a path. */
public val <T> KProperty<T>.projection: String
    get() = path().projection

/** The projection of the property. */
public val String.projection: String
    get() = "\$$this"

/** In order to write `$p.p2` */
@JvmSynthetic public infix fun <T0, T1> KProperty1<T0, T1?>.projectionWith(p2: String): String = "$projection.$p2"

/**
 * Creates a projection of a property whose value is computed from the given expression. Projection with an expression
 * can be used in the following contexts:
 * <ul>
 * <li>$project aggregation pipeline stage.</li>
 * <li>Starting from MongoDB 4.4, it's also accepted in various find-related methods within the {@code
 *   MongoCollection}-based API where projection is supported, for example: <ul>
 * <li>{@code find()}</li>
 * <li>{@code findOneAndReplace()}</li>
 * <li>{@code findOneAndUpdate()}</li>
 * <li>{@code findOneAndDelete()}</li>
 * </ul>
 *
 * </li> </ul>
 *
 * @param expression the expression
 * @param <T> the expression type
 * @return the projection
 * @see #computedSearchMeta(String)
 * @see Aggregates#project(Bson)
 */
@JvmSynthetic
@JvmName("computedFromExt")
public infix fun <T> KProperty<T>.computedFrom(expression: Any): Bson =
    Projections.computed(path(), (expression as? KProperty<*>)?.projection ?: expression)

/**
 * Creates a projection of a property whose value is computed from the given expression. Projection with an expression
 * can be used in the following contexts:
 * <ul>
 * <li>$project aggregation pipeline stage.</li>
 * <li>Starting from MongoDB 4.4, it's also accepted in various find-related methods within the {@code
 *   MongoCollection}-based API where projection is supported, for example: <ul>
 * <li>{@code find()}</li>
 * <li>{@code findOneAndReplace()}</li>
 * <li>{@code findOneAndUpdate()}</li>
 * <li>{@code findOneAndDelete()}</li>
 * </ul>
 *
 * </li> </ul>
 *
 * @param property the data class property
 * @param expression the expression
 * @param <T> the expression type
 * @return the projection
 * @see #computedSearchMeta(String)
 * @see Aggregates#project(Bson)
 */
public fun <T> computedFrom(property: KProperty<T>, expression: Any): Bson = property.computedFrom(expression)

/**
 * Creates a projection of a String whose value is computed from the given expression. Projection with an expression can
 * be used in the following contexts:
 * <ul>
 * <li>$project aggregation pipeline stage.</li>
 * <li>Starting from MongoDB 4.4, it's also accepted in various find-related methods within the {@code
 *   MongoCollection}-based API where projection is supported, for example: <ul>
 * <li>{@code find()}</li>
 * <li>{@code findOneAndReplace()}</li>
 * <li>{@code findOneAndUpdate()}</li>
 * <li>{@code findOneAndDelete()}</li>
 * </ul>
 *
 * </li> </ul>
 *
 * @param expression the expression
 * @return the projection
 * @see #computedSearchMeta(String)
 * @see Aggregates#project(Bson)
 */
@JvmSynthetic
@JvmName("computedFromExt")
public infix fun String.computedFrom(expression: Any): Bson =
    @Suppress("UNCHECKED_CAST") Projections.computed(this, (expression as? KProperty<Any>)?.projection ?: expression)

/**
 * Creates a projection of a String whose value is computed from the given expression. Projection with an expression can
 * be used in the following contexts:
 * <ul>
 * <li>$project aggregation pipeline stage.</li>
 * <li>Starting from MongoDB 4.4, it's also accepted in various find-related methods within the {@code
 *   MongoCollection}-based API where projection is supported, for example: <ul>
 * <li>{@code find()}</li>
 * <li>{@code findOneAndReplace()}</li>
 * <li>{@code findOneAndUpdate()}</li>
 * <li>{@code findOneAndDelete()}</li>
 * </ul>
 *
 * </li> </ul>
 *
 * @param property the data class property
 * @param expression the expression
 * @return the projection
 * @see #computedSearchMeta(String)
 * @see Aggregates#project(Bson)
 */
public fun computedFrom(property: String, expression: Any): Bson = property.computedFrom(expression)

/**
 * Creates a projection that includes all of the given properties.
 *
 * @param properties the field names
 * @return the projection
 */
public fun include(vararg properties: KProperty<*>): Bson = include(properties.asList())

/**
 * Creates a projection that includes all of the given properties.
 *
 * @param properties the field names
 * @return the projection
 */
public fun include(properties: Iterable<KProperty<*>>): Bson = Projections.include(properties.map { it.path() })

/**
 * Creates a projection that excludes all of the given properties.
 *
 * @param properties the field names
 * @return the projection
 */
public fun exclude(vararg properties: KProperty<*>): Bson = exclude(properties.asList())

/**
 * Creates a projection that excludes all of the given properties.
 *
 * @param properties the field names
 * @return the projection
 */
public fun exclude(properties: Iterable<KProperty<*>>): Bson = Projections.exclude(properties.map { it.path() })

/**
 * Creates a projection that excludes the _id field. This suppresses the automatic inclusion of _id that is the default,
 * even when other fields are explicitly included.
 *
 * @return the projection
 */
public fun excludeId(): Bson = Projections.excludeId()

/**
 * Creates a projection that includes for the given property only the first element of an array that matches the query
 * filter. This is referred to as the positional $ operator.
 *
 * @return the projection @mongodb.driver.manual reference/operator/projection/positional/#projection Project the first
 *   matching element ($ operator)
 */
public val <T> KProperty<T>.elemMatchProj: Bson
    get() = Projections.elemMatch(path())

/**
 * Creates a projection that includes for the given property only the first element of the array value of that field
 * that matches the given query filter.
 *
 * @param filter the filter to apply
 * @return the projection @mongodb.driver.manual reference/operator/projection/elemMatch elemMatch
 */
@JvmSynthetic
@JvmName("elemMatchProjExt")
public infix fun <T> KProperty<T>.elemMatchProj(filter: Bson): Bson = Projections.elemMatch(path(), filter)

/**
 * Creates a projection that includes for the given property only the first element of the array value of that field
 * that matches the given query filter.
 *
 * @param property the data class property
 * @param filter the filter to apply
 * @return the projection @mongodb.driver.manual reference/operator/projection/elemMatch elemMatch
 */
public fun <T> elemMatchProj(property: KProperty<T>, filter: Bson): Bson = property.elemMatchProj(filter)

/**
 * Creates a $meta projection for the given property
 *
 * @param metaFieldName the meta field name
 * @return the projection @mongodb.driver.manual reference/operator/aggregation/meta/
 * @see #metaTextScore(String)
 * @see #metaSearchScore(String)
 * @see #metaVectorSearchScore(String)
 * @see #metaSearchHighlights(String)
 * @since 4.1
 */
@JvmSynthetic
@JvmName("metaExt")
public infix fun <T> KProperty<T>.meta(metaFieldName: String): Bson = Projections.meta(path(), metaFieldName)

/**
 * Creates a $meta projection for the given property
 *
 * @param property the data class property
 * @param metaFieldName the meta field name
 * @return the projection @mongodb.driver.manual reference/operator/aggregation/meta/
 * @see #metaTextScore(String)
 * @see #metaSearchScore(String)
 * @see #metaVectorSearchScore(String)
 * @see #metaSearchHighlights(String)
 * @since 4.1
 */
public fun <T> meta(property: KProperty<T>, metaFieldName: String): Bson = property.meta(metaFieldName)

/**
 * Creates a textScore projection for the given property, for use with text queries. Calling this method is equivalent
 * to calling {@link #meta(String)} with {@code "textScore"} as the argument.
 *
 * @return the projection
 * @see Filters#text(String, TextSearchOptions) @mongodb.driver.manual
 *   reference/operator/aggregation/meta/#text-score-metadata--meta---textscore- textScore
 */
public fun <T> KProperty<T>.metaTextScore(): Bson = Projections.metaTextScore(path())

/**
 * Creates a searchScore projection for the given property, for use with {@link Aggregates#search(SearchOperator,
 * SearchOptions)} / {@link Aggregates#search(SearchCollector, SearchOptions)}. Calling this method is equivalent to
 * calling {@link #meta(String, String)} with {@code "searchScore"} as the argument.
 *
 * @return the projection @mongodb.atlas.manual atlas-search/scoring/ Scoring
 * @since 4.7
 */
public fun <T> KProperty<T>.metaSearchScore(): Bson = Projections.metaSearchScore(path())

/**
 * Creates a vectorSearchScore projection for the given property, for use with {@link
 * Aggregates#vectorSearch(FieldSearchPath, Iterable, String, long, VectorSearchOptions)} . Calling this method is
 * equivalent to calling {@link #meta(String, String)} with {@code "vectorSearchScore"} as the argument.
 *
 * @return the projection @mongodb.atlas.manual atlas-search/scoring/ Scoring @mongodb.server.release 6.0.10
 * @since 4.11
 */
@Beta(Reason.SERVER)
public fun <T> KProperty<T>.metaVectorSearchScore(): Bson = Projections.metaVectorSearchScore(path())

/**
 * Creates a searchHighlights projection for the given property, for use with {@link Aggregates#search(SearchOperator,
 * SearchOptions)} / {@link Aggregates#search(SearchCollector, SearchOptions)}. Calling this method is equivalent to
 * calling {@link #meta(String, String)} with {@code "searchHighlights"} as the argument.
 *
 * @return the projection
 * @see com.mongodb.client.model.search.SearchHighlight @mongodb.atlas.manual atlas-search/highlighting/ Highlighting
 * @since 4.7
 */
public fun <T> KProperty<T>.metaSearchHighlights(): Bson = Projections.metaSearchHighlights(path())

/**
 * Creates a projection to the given property of a slice of the array value of that field.
 *
 * @param limit the number of elements to project.
 * @return the projection @mongodb.driver.manual reference/operator/projection/slice Slice
 */
public infix fun <T> KProperty<T>.slice(limit: Int): Bson = Projections.slice(path(), limit)

/**
 * Creates a projection to the given property of a slice of the array value of that field.
 *
 * @param skip the number of elements to skip before applying the limit
 * @param limit the number of elements to project
 * @return the projection @mongodb.driver.manual reference/operator/projection/slice Slice
 */
public fun <T> KProperty<T>.slice(skip: Int, limit: Int): Bson = Projections.slice(path(), skip, limit)

/**
 * Creates a projection that combines the list of projections into a single one. If there are duplicate keys, the last
 * one takes precedence.
 *
 * @param projections the list of projections to combine
 * @return the combined projection
 */
public fun fields(vararg projections: Bson): Bson = Projections.fields(*projections)

/**
 * Creates a projection that combines the list of projections into a single one. If there are duplicate keys, the last
 * one takes precedence.
 *
 * @param projections the list of projections to combine
 * @return the combined projection @mongodb.driver.manual
 */
public fun fields(projections: List<Bson>): Bson = Projections.fields(projections)
