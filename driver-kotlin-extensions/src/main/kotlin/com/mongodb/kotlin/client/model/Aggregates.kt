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

import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.GraphLookupOptions
import com.mongodb.client.model.MergeOptions
import com.mongodb.client.model.UnwindOptions
import com.mongodb.client.model.densify.DensifyOptions
import com.mongodb.client.model.densify.DensifyRange
import com.mongodb.kotlin.client.model.Projections.projection
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1
import org.bson.conversions.Bson

/**
 * Aggregates extension methods to improve Kotlin interop
 *
 * @since 5.3
 */
public object Aggregates {
    /**
     * Creates a $count pipeline stage using the named field to store the result
     *
     * @param property the data class field in which to store the count
     * @return the $count pipeline stage @mongodb.driver.manual reference/operator/aggregation/count/ $count
     */
    public fun <T> count(property: KProperty<T>): Bson = Aggregates.count(property.path())

    /**
     * Creates a $lookup pipeline stage, joining the current collection with the one specified in from using the given
     * pipeline. If the first stage in the pipeline is a {@link Aggregates#documents(List) $documents} stage, then the
     * {@code from} collection is ignored.
     *
     * @param from the collection in the same database to perform the join with.
     * @param localField the data class field from the local collection to match values against.
     * @param foreignField the data class field in the from collection to match values against.
     * @param pipeline the pipeline to run on the joined collection.
     * @param as the name of the new array field to add to the input documents.
     * @return the $lookup pipeline stage @mongodb.driver.manual reference/operator/aggregation/lookup/
     *   $lookup @mongodb.server.release 3.6
     */
    public fun <FROM : Any> lookup(
        from: com.mongodb.kotlin.client.MongoCollection<FROM>,
        localField: KProperty1<out Any, Any?>,
        foreignField: KProperty1<FROM, Any?>,
        newAs: String
    ): Bson = Aggregates.lookup(from.namespace.collectionName, localField.path(), foreignField.path(), newAs)

    /**
     * Creates a $lookup pipeline stage, joining the current collection with the one specified in from using the given
     * pipeline. If the first stage in the pipeline is a {@link Aggregates#documents(List) $documents} stage, then the
     * {@code from} collection is ignored.
     *
     * @param from the collection in the same database to perform the join with.
     * @param localField the data class field from the local collection to match values against.
     * @param foreignField the data class field in the from collection to match values against.
     * @param pipeline the pipeline to run on the joined collection.
     * @param as the name of the new array field to add to the input documents.
     * @return the $lookup pipeline stage @mongodb.driver.manual reference/operator/aggregation/lookup/
     *   $lookup @mongodb.server.release 3.6
     */
    public fun <FROM : Any> lookup(
        from: com.mongodb.kotlin.client.coroutine.MongoCollection<FROM>,
        localField: KProperty1<out Any, Any?>,
        foreignField: KProperty1<FROM, Any?>,
        newAs: String
    ): Bson = Aggregates.lookup(from.namespace.collectionName, localField.path(), foreignField.path(), newAs)

    /**
     * Creates a graphLookup pipeline stage for the specified filter
     *
     * @param <TExpression> the expression type
     * @param from the collection to query
     * @param startWith the expression to start the graph lookup with
     * @param connectFromField the data class from field
     * @param connectToField the data class to field
     * @param fieldAs name of field in output document
     * @param options optional values for the graphLookup
     * @return the $graphLookup pipeline stage @mongodb.driver.manual reference/operator/aggregation/graphLookup/
     *   $graphLookup @mongodb.server.release 3.4
     */
    @Suppress("LongParameterList")
    public fun <TExpression, FROM : Any> graphLookup(
        from: com.mongodb.kotlin.client.MongoCollection<FROM>,
        startWith: TExpression,
        connectFromField: KProperty1<FROM, Any?>,
        connectToField: KProperty1<FROM, Any?>,
        fieldAs: String,
        options: GraphLookupOptions = GraphLookupOptions()
    ): Bson =
        Aggregates.graphLookup(
            from.namespace.collectionName, startWith, connectFromField.path(), connectToField.path(), fieldAs, options)

    /**
     * Creates a graphLookup pipeline stage for the specified filter
     *
     * @param <TExpression> the expression type
     * @param from the collection to query
     * @param startWith the expression to start the graph lookup with
     * @param connectFromField the data class from field
     * @param connectToField the data class to field
     * @param fieldAs name of field in output document
     * @param options optional values for the graphLookup
     * @return the $graphLookup pipeline stage @mongodb.driver.manual reference/operator/aggregation/graphLookup/
     *   $graphLookup @mongodb.server.release 3.4
     */
    @Suppress("LongParameterList")
    public fun <TExpression, FROM : Any> graphLookup(
        from: com.mongodb.kotlin.client.coroutine.MongoCollection<FROM>,
        startWith: TExpression,
        connectFromField: KProperty1<FROM, Any?>,
        connectToField: KProperty1<FROM, Any?>,
        fieldAs: String,
        options: GraphLookupOptions = GraphLookupOptions()
    ): Bson =
        Aggregates.graphLookup(
            from.namespace.collectionName, startWith, connectFromField.path(), connectToField.path(), fieldAs, options)

    /**
     * Creates a $unionWith pipeline stage.
     *
     * @param collection the collection in the same database to perform the union with.
     * @param pipeline the pipeline to run on the union.
     * @return the $unionWith pipeline stage @mongodb.driver.manual reference/operator/aggregation/unionWith/
     *   $unionWith @mongodb.server.release 4.4
     */
    public fun unionWith(collection: com.mongodb.kotlin.client.MongoCollection<*>, pipeline: List<Bson>): Bson =
        Aggregates.unionWith(collection.namespace.collectionName, pipeline)

    /**
     * Creates a $unionWith pipeline stage.
     *
     * @param collection the collection in the same database to perform the union with.
     * @param pipeline the pipeline to run on the union.
     * @return the $unionWith pipeline stage @mongodb.driver.manual reference/operator/aggregation/unionWith/
     *   $unionWith @mongodb.server.release 4.4
     */
    public fun unionWith(
        collection: com.mongodb.kotlin.client.coroutine.MongoCollection<*>,
        pipeline: List<Bson>
    ): Bson = Aggregates.unionWith(collection.namespace.collectionName, pipeline)

    /**
     * Creates a $unwind pipeline stage for the specified field name, which must be prefixed by a {@code '$'} sign.
     *
     * @param property the data class field name
     * @param unwindOptions options for the unwind pipeline stage
     * @return the $unwind pipeline stage @mongodb.driver.manual reference/operator/aggregation/unwind/ $unwind
     */
    public fun <T> unwind(property: KProperty<Iterable<T>?>, unwindOptions: UnwindOptions = UnwindOptions()): Bson {
        return if (unwindOptions == UnwindOptions()) {
            Aggregates.unwind(property.projection)
        } else {
            Aggregates.unwind(property.projection, unwindOptions)
        }
    }

    /**
     * Creates a $out pipeline stage that writes into the specified collection
     *
     * @param collection the collection
     * @return the $out pipeline stage @mongodb.driver.manual reference/operator/aggregation/out/ $out
     */
    public fun out(collection: com.mongodb.kotlin.client.MongoCollection<*>): Bson =
        Aggregates.out(collection.namespace.collectionName)

    /**
     * Creates a $out pipeline stage that writes into the specified collection
     *
     * @param collection the collection
     * @return the $out pipeline stage @mongodb.driver.manual reference/operator/aggregation/out/ $out
     */
    public fun out(collection: com.mongodb.kotlin.client.coroutine.MongoCollection<*>): Bson =
        Aggregates.out(collection.namespace.collectionName)

    /**
     * Creates a $merge pipeline stage that merges into the specified collection
     *
     * @param collection the collection to merge into
     * @param options the merge options
     * @return the $merge pipeline stage @mongodb.driver.manual reference/operator/aggregation/merge/
     *   $merge @mongodb.server.release 4.2
     */
    public fun merge(
        collection: com.mongodb.kotlin.client.MongoCollection<*>,
        options: MergeOptions = MergeOptions()
    ): Bson = Aggregates.merge(collection.namespace.collectionName, options)

    /**
     * Creates a $merge pipeline stage that merges into the specified collection
     *
     * @param collection the collection to merge into
     * @param options the merge options
     * @return the $merge pipeline stage @mongodb.driver.manual reference/operator/aggregation/merge/
     *   $merge @mongodb.server.release 4.2
     */
    public fun merge(
        collection: com.mongodb.kotlin.client.coroutine.MongoCollection<*>,
        options: MergeOptions = MergeOptions()
    ): Bson = Aggregates.merge(collection.namespace.collectionName, options)

    /**
     * Creates a `$densify` pipeline stage, which adds documents to a sequence of documents where certain values in the
     * `field` are missing.
     *
     * @param field The field to densify.
     * @param range The range.
     * @return The requested pipeline stage. @mongodb.driver.manual reference/operator/aggregation/densify/
     *   $densify @mongodb.driver.manual core/document/#dot-notation Dot notation @mongodb.server.release 5.1
     */
    public fun <T> densify(property: KProperty<T>, range: DensifyRange): Bson =
        Aggregates.densify(property.path(), range)

    /**
     * Creates a {@code $densify} pipeline stage, which adds documents to a sequence of documents where certain values
     * in the {@code field} are missing.
     *
     * @param field The field to densify.
     * @param range The range.
     * @param options The densify options. Specifying {@link DensifyOptions#densifyOptions()} is equivalent to calling
     *   {@link #densify(String, DensifyRange)}.
     * @return The requested pipeline stage. @mongodb.driver.manual reference/operator/aggregation/densify/
     *   $densify @mongodb.driver.manual core/document/#dot-notation Dot notation @mongodb.server.release 5.1
     */
    public fun <T> densify(property: KProperty<T>, range: DensifyRange, options: DensifyOptions): Bson =
        Aggregates.densify(property.path(), range, options)
}
