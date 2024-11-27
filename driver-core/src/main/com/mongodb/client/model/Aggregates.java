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

package com.mongodb.client.model;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.densify.DensifyOptions;
import com.mongodb.client.model.densify.DensifyRange;
import com.mongodb.client.model.fill.FillOptions;
import com.mongodb.client.model.fill.FillOutputField;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.search.FieldSearchPath;
import com.mongodb.client.model.search.SearchCollector;
import com.mongodb.client.model.search.SearchOperator;
import com.mongodb.client.model.search.SearchOptions;
import com.mongodb.client.model.search.VectorSearchOptions;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.Vector;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.model.GeoNearOptions.geoNearOptions;
import static com.mongodb.client.model.densify.DensifyOptions.densifyOptions;
import static com.mongodb.client.model.search.SearchOptions.searchOptions;
import static com.mongodb.internal.Iterables.concat;
import static com.mongodb.internal.client.model.Util.sizeAtLeast;
import static java.util.Arrays.asList;

/**
 * Builders for aggregation pipeline stages.
 *
 * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation pipeline
 * @mongodb.server.release 2.2
 * @since 3.1
 */
@SuppressWarnings("overloads")
public final class Aggregates {

    /**
     * Creates an $addFields pipeline stage
     *
     * @param fields        the fields to add
     * @return the $addFields pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/addFields/ $addFields
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson addFields(final Field<?>... fields) {
        return addFields(asList(fields));
    }

    /**
     * Creates an $addFields pipeline stage
     *
     * @param fields        the fields to add
     * @return the $addFields pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/addFields/ $addFields
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson addFields(final List<Field<?>> fields) {
        return new FieldsStage("$addFields", fields);
    }

    /**
     * Creates a $set pipeline stage for the specified projection
     *
     * @param fields the fields to add
     * @return the $set pipeline stage
     * @see Projections
     * @since 4.3
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual reference/operator/aggregation/set/ $set
     */
    public static Bson set(final Field<?>... fields) {
        return set(asList(fields));
    }

    /**
     * Creates a $set pipeline stage for the specified projection
     *
     * @param fields the fields to add
     * @return the $set pipeline stage
     * @see Projections
     * @since 4.3
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual reference/operator/aggregation/set/ $set
     */
    public static Bson set(final List<Field<?>> fields) {
        return new FieldsStage("$set", fields);
    }


    /**
     * Creates a $bucket pipeline stage
     *
     * @param <TExpression> the groupBy expression type
     * @param <Boundary>    the boundary type
     * @param groupBy       the criteria to group By
     * @param boundaries    the boundaries of the buckets
     * @return the $bucket pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/bucket/ $bucket
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression, Boundary> Bson bucket(final TExpression groupBy, final List<Boundary> boundaries) {
        return bucket(groupBy, boundaries, new BucketOptions());
    }

    /**
     * Creates a $bucket pipeline stage
     *
     * @param <TExpression> the groupBy expression type
     * @param <TBoundary>    the boundary type
     * @param groupBy       the criteria to group By
     * @param boundaries    the boundaries of the buckets
     * @param options       the optional values for the $bucket stage
     * @return the $bucket pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/bucket/ $bucket
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression, TBoundary> Bson bucket(final TExpression groupBy, final List<TBoundary> boundaries,
                                                       final BucketOptions options) {
        return new BucketStage<>(groupBy, boundaries, options);
    }

    /**
     * Creates a $bucketAuto pipeline stage
     *
     * @param <TExpression> the groupBy expression type
     * @param groupBy       the criteria to group By
     * @param buckets       the number of the buckets
     * @return the $bucketAuto pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/bucketAuto/ $bucketAuto
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson bucketAuto(final TExpression groupBy, final int buckets) {
        return bucketAuto(groupBy, buckets, new BucketAutoOptions());
    }

    /**
     * Creates a $bucketAuto pipeline stage
     *
     * @param <TExpression> the groupBy expression type
     * @param groupBy       the criteria to group By
     * @param buckets       the number of the buckets
     * @param options       the optional values for the $bucketAuto stage
     * @return the $bucketAuto pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/bucketAuto/ $bucketAuto
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson bucketAuto(final TExpression groupBy, final int buckets, final BucketAutoOptions options) {
        return new BucketAutoStage<>(groupBy, buckets, options);
    }

    /**
     * Creates a $count pipeline stage using the field name "count" to store the result
     *
     * @return the $count pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/count/ $count
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson count() {
        return count("count");
    }

    /**
     * Creates a $count pipeline stage using the named field to store the result
     *
     * @param field the field in which to store the count
     * @return the $count pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/count/ $count
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson count(final String field) {
        return new BsonDocument("$count", new BsonString(field));
    }

    /**
     * Creates a $match pipeline stage for the specified filter
     *
     * @param filter the filter to match
     * @return the $match pipeline stage
     * @see Filters
     * @mongodb.driver.manual reference/operator/aggregation/match/ $match
     */
    public static Bson match(final Bson filter) {
        return new SimplePipelineStage("$match", filter);
    }

    /**
     * Creates a $project pipeline stage for the specified projection
     *
     * @param projection the projection
     * @return the $project pipeline stage
     * @see Projections
     * @mongodb.driver.manual reference/operator/aggregation/project/ $project
     */
    public static Bson project(final Bson projection) {
        return new SimplePipelineStage("$project", projection);
    }

    /**
     * Creates a $sort pipeline stage for the specified sort specification
     *
     * @param sort the sort specification
     * @return the $sort pipeline stage
     * @see Sorts
     * @mongodb.driver.manual reference/operator/aggregation/sort/#sort-aggregation $sort
     */
    public static Bson sort(final Bson sort) {
        return new SimplePipelineStage("$sort", sort);
    }

    /**
     * Creates a $sortByCount pipeline stage for the specified filter
     *
     * @param <TExpression> the expression type
     * @param filter        the filter specification
     * @return the $sortByCount pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/sortByCount/ $sortByCount
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson sortByCount(final TExpression filter) {
        return new SortByCountStage<>(filter);
    }

    /**
     * Creates a $skip pipeline stage
     *
     * @param skip the number of documents to skip
     * @return the $skip pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/skip/ $skip
     */
    public static Bson skip(final int skip) {
        return new BsonDocument("$skip", new BsonInt32(skip));
    }

    /**
     * Creates a $limit pipeline stage for the specified filter
     *
     * @param limit the limit
     * @return the $limit pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/limit/  $limit
     */
    public static Bson limit(final int limit) {
        return new BsonDocument("$limit", new BsonInt32(limit));
    }

    /**
     * Creates a $lookup pipeline stage, joining the current collection with the one specified in from
     * using equality match between the local field and the foreign field
     *
     * @param from         the name of the collection in the same database to perform the join with.
     * @param localField   the field from the local collection to match values against.
     * @param foreignField the field in the from collection to match values against.
     * @param as           the name of the new array field to add to the input documents.
     * @return the $lookup pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/lookup/ $lookup
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public static Bson lookup(final String from, final String localField, final String foreignField, final String as) {
        return new BsonDocument("$lookup", new BsonDocument("from", new BsonString(from))
                                                   .append("localField", new BsonString(localField))
                                                   .append("foreignField", new BsonString(foreignField))
                                                   .append("as", new BsonString(as)));
    }

    /**
     * Creates a $lookup pipeline stage, joining the current collection with the
     * one specified in from using the given pipeline. If the first stage in the
     * pipeline is a {@link Aggregates#documents(List) $documents} stage, then
     * the {@code from} collection is ignored.
     *
     * @param from          the name of the collection in the same database to
     *                      perform the join with. Must be {$code null} if the
     *                      first pipeline stage is $documents.
     * @param pipeline      the pipeline to run on the joined collection.
     * @param as            the name of the new array field to add to the input documents.
     * @return the $lookup pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/lookup/ $lookup
     * @mongodb.server.release 3.6
     * @since 3.7
     *
     */
    public static Bson lookup(@Nullable final String from, final List<? extends Bson> pipeline, final String as) {
        return lookup(from, null, pipeline, as);
    }

    /**
     * Creates a $lookup pipeline stage, joining the current collection with the
     * one specified in from using the given pipeline. If the first stage in the
     * pipeline is a {@link Aggregates#documents(List) $documents} stage, then
     * the {@code from} collection is ignored.
     *
     * @param <TExpression> the Variable value expression type
     * @param from          the name of the collection in the same database to
     *                      perform the join with. Must be {$code null} if the
     *                      first pipeline stage is $documents.
     * @param let           the variables to use in the pipeline field stages.
     * @param pipeline      the pipeline to run on the joined collection.
     * @param as            the name of the new array field to add to the input documents.
     * @return the $lookup pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/lookup/ $lookup
     * @mongodb.server.release 3.6
     * @since 3.7
     */
    public static <TExpression> Bson lookup(@Nullable final String from, @Nullable final List<Variable<TExpression>> let,
                                            final List<? extends Bson> pipeline, final String as) {
       return new LookupStage<>(from, let, pipeline, as);
    }

    /**
     * Creates a facet pipeline stage
     *
     * @param facets the facets to use
     * @return the new pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/facet/ $facet
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson facet(final List<Facet> facets) {
        return new FacetStage(facets);
    }

    /**
     * Creates a facet pipeline stage
     *
     * @param facets the facets to use
     * @return the new pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/facet/ $facet
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static Bson facet(final Facet... facets) {
        return new FacetStage(asList(facets));
    }

    /**
     * Creates a graphLookup pipeline stage for the specified filter
     *
     * @param <TExpression>     the expression type
     * @param from             the collection to query
     * @param startWith        the expression to start the graph lookup with
     * @param connectFromField the from field
     * @param connectToField   the to field
     * @param as               name of field in output document
     * @return the $graphLookup pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/graphLookup/ $graphLookup
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson graphLookup(final String from, final TExpression startWith, final String connectFromField,
                                                 final String connectToField, final String as) {
        return graphLookup(from, startWith, connectFromField, connectToField, as, new GraphLookupOptions());
    }

    /**
     * Creates a graphLookup pipeline stage for the specified filter
     *
     * @param <TExpression>    the expression type
     * @param from             the collection to query
     * @param startWith        the expression to start the graph lookup with
     * @param connectFromField the from field
     * @param connectToField   the to field
     * @param as               name of field in output document
     * @param options          optional values for the graphLookup
     * @return the $graphLookup pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/graphLookup/ $graphLookup
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson graphLookup(final String from, final TExpression startWith, final String connectFromField,
                                                 final String connectToField, final String as, final GraphLookupOptions options) {
        notNull("options", options);
        return new GraphLookupStage<>(from, startWith, connectFromField, connectToField, as, options);
    }

    /**
     * Creates a $group pipeline stage for the specified filter
     *
     * @param <TExpression>     the expression type
     * @param id                the id expression for the group, which may be null
     * @param fieldAccumulators zero or more field accumulator pairs
     * @return the $group pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/group/ $group
     * @mongodb.driver.manual meta/aggregation-quick-reference/#aggregation-expressions Expressions
     */
    public static <TExpression> Bson group(@Nullable final TExpression id, final BsonField... fieldAccumulators) {
        return group(id, asList(fieldAccumulators));
    }

    /**
     * Creates a $group pipeline stage for the specified filter
     *
     * @param <TExpression>     the expression type
     * @param id                the id expression for the group, which may be null
     * @param fieldAccumulators zero or more field accumulator pairs
     * @return the $group pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/group/ $group
     * @mongodb.driver.manual meta/aggregation-quick-reference/#aggregation-expressions Expressions
     */
    public static <TExpression> Bson group(@Nullable final TExpression id, final List<BsonField> fieldAccumulators) {
        return new GroupStage<>(id, fieldAccumulators);
    }

    /**
     * Creates a $unionWith pipeline stage.
     *
     * @param collection    the name of the collection in the same database to perform the union with.
     * @param pipeline      the pipeline to run on the union.
     * @return the $unionWith pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/unionWith/ $unionWith
     * @mongodb.server.release 4.4
     * @since 4.1
     */
    public static Bson unionWith(final String collection, final List<? extends Bson> pipeline) {
        return new UnionWithStage(collection, pipeline);
    }

    /**
     * Creates a $unwind pipeline stage for the specified field name, which must be prefixed by a {@code '$'} sign.
     *
     * @param fieldName the field name, prefixed by a {@code '$' sign}
     * @return the $unwind pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/unwind/ $unwind
     */
    public static Bson unwind(final String fieldName) {
        return new BsonDocument("$unwind", new BsonString(fieldName));
    }

    /**
     * Creates a $unwind pipeline stage for the specified field name, which must be prefixed by a {@code '$'} sign.
     *
     * @param fieldName     the field name, prefixed by a {@code '$' sign}
     * @param unwindOptions options for the unwind pipeline stage
     * @return the $unwind pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/unwind/ $unwind
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public static Bson unwind(final String fieldName, final UnwindOptions unwindOptions) {
        notNull("unwindOptions", unwindOptions);
        BsonDocument options = new BsonDocument("path", new BsonString(fieldName));
        Boolean preserveNullAndEmptyArrays = unwindOptions.isPreserveNullAndEmptyArrays();
        if (preserveNullAndEmptyArrays != null) {
            options.append("preserveNullAndEmptyArrays", BsonBoolean.valueOf(preserveNullAndEmptyArrays));
        }
        String includeArrayIndex = unwindOptions.getIncludeArrayIndex();
        if (includeArrayIndex != null) {
            options.append("includeArrayIndex", new BsonString(includeArrayIndex));
        }
        return new BsonDocument("$unwind", options);
    }

    /**
     * Creates a $out pipeline stage that writes into the specified collection
     *
     * @param collectionName the collection name
     * @return the $out pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/out/  $out
     */
    public static Bson out(final String collectionName) {
        return new BsonDocument("$out", new BsonString(collectionName));
    }

    /**
     * Creates a $out pipeline stage that supports outputting to a different database.
     *
     * @param databaseName   the database name
     * @param collectionName the collection name
     * @return the $out pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/out/  $out
     * @mongodb.server.release 4.4
     * @since 4.1
     */
    public static Bson out(final String databaseName, final String collectionName) {
        return new BsonDocument("$out", new BsonDocument("db", new BsonString(databaseName))
                .append("coll", new BsonString(collectionName)));
    }

    /**
     * Creates a $out pipeline stage that writes out to the specified destination
     *
     * @param destination the destination details
     * @return the $out pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/out/  $out
     * @since 4.1
     */
    public static Bson out(final Bson destination) {
        return new SimplePipelineStage("$out", destination);
    }

    /**
     * Creates a $merge pipeline stage that merges into the specified collection
     *
     * @param collectionName the name of the collection to merge into
     * @return the $merge pipeline stage
     * @since 3.11
     * @mongodb.driver.manual reference/operator/aggregation/merge/  $merge
     * @mongodb.server.release 4.2
     */
    public static Bson merge(final String collectionName) {
        return merge(collectionName, new MergeOptions());
    }

    /**
     * Creates a $merge pipeline stage that merges into the specified namespace
     *
     * @param namespace the namespace to merge into
     * @return the $merge pipeline stage
     * @since 3.11
     * @mongodb.driver.manual reference/operator/aggregation/merge/  $merge
     * @mongodb.server.release 4.2
     */
    public static Bson merge(final MongoNamespace namespace) {
        return merge(namespace, new MergeOptions());
    }

    /**
     * Creates a $merge pipeline stage that merges into the specified collection using the specified options.
     *
     * @param collectionName the name of the collection to merge into
     * @param options the merge options
     * @return the $merge pipeline stage
     * @since 3.11
     * @mongodb.driver.manual reference/operator/aggregation/merge/  $merge
     * @mongodb.server.release 4.2
     */
    public static Bson merge(final String collectionName, final MergeOptions options) {
        return new MergeStage(new BsonString(collectionName), options);
    }

    /**
     * Creates a $merge pipeline stage that merges into the specified namespace using the specified options.
     *
     * @param namespace the namespace to merge into
     * @param options the merge options
     * @return the $merge pipeline stage
     * @since 3.11
     * @mongodb.driver.manual reference/operator/aggregation/merge/  $merge
     * @mongodb.server.release 4.2
     */
    public static Bson merge(final MongoNamespace namespace, final MergeOptions options) {
        return new MergeStage(new BsonDocument("db", new BsonString(namespace.getDatabaseName()))
                .append("coll", new BsonString(namespace.getCollectionName())), options);
    }

    /**
     * Creates a $replaceRoot pipeline stage
     *
     * @param <TExpression> the new root type
     * @param value         the new root value
     * @return the $replaceRoot pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/replaceRoot/ $replaceRoot
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public static <TExpression> Bson replaceRoot(final TExpression value) {
        return new ReplaceStage<>(value);
    }

    /**
     * Creates a $replaceRoot pipeline stage
     *
     * <p>With $replaceWith, you can promote an embedded document to the top-level.
     * You can also specify a new document as the replacement.</p>
     *
     * <p>The $replaceWith is an alias for {@link #replaceRoot(Object)}.</p>
     *
     * @param <TExpression> the new root type
     * @param value         the new root value
     * @return the $replaceRoot pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/replaceWith/ $replaceWith
     * @mongodb.server.release 4.2
     * @since 3.11
     */
    public static <TExpression> Bson replaceWith(final TExpression value) {
        return new ReplaceStage<>(value, true);
    }

    /**
     * Creates a $sample pipeline stage with the specified sample size
     *
     * @param size the sample size
     * @return the $sample pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/sample/  $sample
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public static Bson sample(final int size) {
        return new BsonDocument("$sample", new BsonDocument("size", new BsonInt32(size)));
    }

    /**
     * Creates a {@code $setWindowFields} pipeline stage, which allows using window operators.
     * This stage partitions the input documents similarly to the {@link #group(Object, List) $group} pipeline stage,
     * optionally sorts them, computes fields in the documents by computing window functions over {@linkplain Window windows} specified per
     * function, and outputs the documents. The important difference from the {@code $group} pipeline stage is that
     * documents belonging to the same partition or window are not folded into a single document.
     *
     * @param partitionBy Optional partitioning of data specified like {@code id} in {@link #group(Object, List)}.
     *                    If {@code null}, then all documents belong to the same partition.
     * @param sortBy Fields to sort by. The syntax is identical to {@code sort} in {@link #sort(Bson)} (see {@link Sorts}).
     *               Sorting is required by certain functions and may be required by some windows (see {@link Windows} for more details).
     *               Sorting is used only for the purpose of computing window functions and does not guarantee ordering
     *               of the output documents.
     * @param output A {@linkplain WindowOutputField windowed computation}.
     * @param moreOutput More {@linkplain WindowOutputField windowed computations}.
     * @param <TExpression> The {@code partitionBy} expression type.
     * @return The {@code $setWindowFields} pipeline stage.
     * @mongodb.driver.dochub core/window-functions-set-window-fields $setWindowFields
     * @mongodb.server.release 5.0
     * @since 4.3
     */
    public static <TExpression> Bson setWindowFields(@Nullable final TExpression partitionBy, @Nullable final Bson sortBy,
            final WindowOutputField output, final WindowOutputField... moreOutput) {
        return setWindowFields(partitionBy, sortBy, concat(notNull("output", output), moreOutput));
    }

    /**
     * Creates a {@code $setWindowFields} pipeline stage, which allows using window operators.
     * This stage partitions the input documents similarly to the {@link #group(Object, List) $group} pipeline stage,
     * optionally sorts them, computes fields in the documents by computing window functions over {@linkplain Window windows} specified per
     * function, and outputs the documents. The important difference from the {@code $group} pipeline stage is that
     * documents belonging to the same partition or window are not folded into a single document.
     *
     * @param partitionBy Optional partitioning of data specified like {@code id} in {@link #group(Object, List)}.
     *                    If {@code null}, then all documents belong to the same partition.
     * @param sortBy Fields to sort by. The syntax is identical to {@code sort} in {@link #sort(Bson)} (see {@link Sorts}).
     *               Sorting is required by certain functions and may be required by some windows (see {@link Windows} for more details).
     *               Sorting is used only for the purpose of computing window functions and does not guarantee ordering
     *               of the output documents.
     * @param output A list of {@linkplain WindowOutputField windowed computations}.
     * Specifying an empty list is not an error, but the resulting stage does not do anything useful.
     * @param <TExpression> The {@code partitionBy} expression type.
     * @return The {@code $setWindowFields} pipeline stage.
     * @mongodb.driver.dochub core/window-functions-set-window-fields $setWindowFields
     * @mongodb.server.release 5.0
     * @since 4.3
     */
    public static <TExpression> Bson setWindowFields(@Nullable final TExpression partitionBy, @Nullable final Bson sortBy,
                                                     final Iterable<? extends WindowOutputField> output) {
        notNull("output", output);
        return new SetWindowFieldsStage<>(partitionBy, sortBy, output);
    }

    /**
     * Creates a {@code $densify} pipeline stage, which adds documents to a sequence of documents
     * where certain values in the {@code field} are missing.
     *
     * @param field The field to densify.
     * @param range The range.
     * @return The requested pipeline stage.
     * @mongodb.driver.manual reference/operator/aggregation/densify/ $densify
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     * @mongodb.server.release 5.1
     * @since 4.7
     */
    public static Bson densify(final String field, final DensifyRange range) {
        return densify(notNull("field", field), notNull("range", range), densifyOptions());
    }

    /**
     * Creates a {@code $densify} pipeline stage, which adds documents to a sequence of documents
     * where certain values in the {@code field} are missing.
     *
     * @param field The field to densify.
     * @param range The range.
     * @param options The densify options.
     * Specifying {@link DensifyOptions#densifyOptions()} is equivalent to calling {@link #densify(String, DensifyRange)}.
     * @return The requested pipeline stage.
     * @mongodb.driver.manual reference/operator/aggregation/densify/ $densify
     * @mongodb.driver.manual core/document/#dot-notation Dot notation
     * @mongodb.server.release 5.1
     * @since 4.7
     */
    public static Bson densify(final String field, final DensifyRange range, final DensifyOptions options) {
        notNull("field", field);
        notNull("range", range);
        notNull("options", options);
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                BsonDocument densifySpecificationDoc = new BsonDocument("field", new BsonString(field));
                densifySpecificationDoc.append("range", range.toBsonDocument(documentClass, codecRegistry));
                densifySpecificationDoc.putAll(options.toBsonDocument(documentClass, codecRegistry));
                return new BsonDocument("$densify", densifySpecificationDoc);
            }

            @Override
            public String toString() {
                return "Stage{name='$densify'"
                        + ", field=" + field
                        + ", range=" + range
                        + ", options=" + options
                        + '}';
            }
        };
    }

    /**
     * Creates a {@code $fill} pipeline stage, which assigns values to fields when they are {@link BsonType#NULL Null} or missing.
     *
     * @param options The fill options.
     * @param output The {@link FillOutputField}.
     * @param moreOutput More {@link FillOutputField}s.
     * @return The requested pipeline stage.
     * @mongodb.driver.manual reference/operator/aggregation/fill/ $fill
     * @mongodb.server.release 5.3
     * @since 4.7
     */
    public static Bson fill(final FillOptions options, final FillOutputField output, final FillOutputField... moreOutput) {
        return fill(options, concat(notNull("output", output), moreOutput));
    }

    /**
     * Creates a {@code $fill} pipeline stage, which assigns values to fields when they are {@link BsonType#NULL Null} or missing.
     *
     * @param options The fill options.
     * @param output The non-empty {@link FillOutputField}s.
     * @return The requested pipeline stage.
     * @mongodb.driver.manual reference/operator/aggregation/fill/ $fill
     * @mongodb.server.release 5.3
     * @since 4.7
     */
    public static Bson fill(final FillOptions options, final Iterable<? extends FillOutputField> output) {
        notNull("options", options);
        notNull("output", output);
        isTrueArgument("output must not be empty", sizeAtLeast(output, 1));
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                BsonDocument fillSpecificationDoc = new BsonDocument();
                fillSpecificationDoc.putAll(options.toBsonDocument(documentClass, codecRegistry));
                BsonDocument outputDoc = new BsonDocument();
                for (final FillOutputField computation : output) {
                    BsonDocument computationDoc = computation.toBsonDocument(documentClass, codecRegistry);
                    assertTrue(computationDoc.size() == 1);
                    outputDoc.putAll(computationDoc);
                }
                fillSpecificationDoc.append("output", outputDoc);
                return new BsonDocument("$fill", fillSpecificationDoc);
            }

            @Override
            public String toString() {
                return "Stage{name='$fill'"
                        + ", options=" + options
                        + ", output=" + output
                        + '}';
            }
        };
    }

    /**
     * Creates a {@code $search} pipeline stage supported by MongoDB Atlas.
     * You may use the {@code $meta: "searchScore"} expression, e.g., via {@link Projections#metaSearchScore(String)},
     * to extract the relevance score assigned to each found document.
     * <p>
     * {@link Filters#text(String, TextSearchOptions)} is a legacy text search alternative.</p>
     *
     * @param operator A search operator.
     * @return The {@code $search} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-search/query-syntax/#-search $search
     * @mongodb.atlas.manual atlas-search/operators-and-collectors/#operators Search operators
     * @mongodb.atlas.manual atlas-search/scoring/ Scoring
     * @since 4.7
     */
    public static Bson search(final SearchOperator operator) {
        return search(operator, searchOptions());
    }

    /**
     * Creates a {@code $search} pipeline stage supported by MongoDB Atlas.
     * You may use the {@code $meta: "searchScore"} expression, e.g., via {@link Projections#metaSearchScore(String)},
     * to extract the relevance score assigned to each found document.
     * <p>
     * {@link Filters#text(String, TextSearchOptions)} is a legacy text search alternative.</p>
     *
     * @param operator A search operator.
     * @param options Optional {@code $search} pipeline stage fields.
     * Specifying {@link SearchOptions#searchOptions()} is equivalent to calling {@link #search(SearchOperator)}.
     * @return The {@code $search} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-search/query-syntax/#-search $search
     * @mongodb.atlas.manual atlas-search/operators-and-collectors/#operators Search operators
     * @mongodb.atlas.manual atlas-search/scoring/ Scoring
     * @since 4.7
     */
    public static Bson search(final SearchOperator operator, final SearchOptions options) {
        return new SearchStage("$search", notNull("operator", operator), notNull("options", options));
    }

    /**
     * Creates a {@code $search} pipeline stage supported by MongoDB Atlas.
     * You may use {@code $meta: "searchScore"}, e.g., via {@link Projections#metaSearchScore(String)},
     * to extract the relevance score assigned to each found document.
     *
     * @param collector A search collector.
     * @return The {@code $search} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-search/query-syntax/#-search $search
     * @mongodb.atlas.manual atlas-search/operators-and-collectors/#collectors Search collectors
     * @mongodb.atlas.manual atlas-search/scoring/ Scoring
     * @since 4.7
     */
    public static Bson search(final SearchCollector collector) {
        return search(collector, searchOptions());
    }

    /**
     * Creates a {@code $search} pipeline stage supported by MongoDB Atlas.
     * You may use {@code $meta: "searchScore"}, e.g., via {@link Projections#metaSearchScore(String)},
     * to extract the relevance score assigned to each found document.
     *
     * @param collector A search collector.
     * @param options Optional {@code $search} pipeline stage fields.
     * Specifying {@link SearchOptions#searchOptions()} is equivalent to calling {@link #search(SearchCollector)}.
     * @return The {@code $search} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-search/query-syntax/#-search $search
     * @mongodb.atlas.manual atlas-search/operators-and-collectors/#collectors Search collectors
     * @mongodb.atlas.manual atlas-search/scoring/ Scoring
     * @since 4.7
     */
    public static Bson search(final SearchCollector collector, final SearchOptions options) {
        return new SearchStage("$search", notNull("collector", collector), notNull("options", options));
    }

    /**
     * Creates a {@code $searchMeta} pipeline stage supported by MongoDB Atlas.
     * Unlike {@link #search(SearchOperator) $search}, it does not return found documents,
     * instead it returns metadata, which in case of using the {@code $search} stage
     * may be extracted by using {@code $$SEARCH_META} variable, e.g., via {@link Projections#computedSearchMeta(String)}.
     *
     * @param operator A search operator.
     * @return The {@code $searchMeta} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-search/query-syntax/#-searchmeta $searchMeta
     * @mongodb.atlas.manual atlas-search/operators-and-collectors/#operators Search operators
     * @since 4.7
     */
    public static Bson searchMeta(final SearchOperator operator) {
        return searchMeta(operator, searchOptions());
    }

    /**
     * Creates a {@code $searchMeta} pipeline stage supported by MongoDB Atlas.
     * Unlike {@link #search(SearchOperator, SearchOptions) $search}, it does not return found documents,
     * instead it returns metadata, which in case of using the {@code $search} stage
     * may be extracted by using {@code $$SEARCH_META} variable, e.g., via {@link Projections#computedSearchMeta(String)}.
     *
     * @param operator A search operator.
     * @param options Optional {@code $search} pipeline stage fields.
     * Specifying {@link SearchOptions#searchOptions()} is equivalent to calling {@link #searchMeta(SearchOperator)}.
     * @return The {@code $searchMeta} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-search/query-syntax/#-searchmeta $searchMeta
     * @mongodb.atlas.manual atlas-search/operators-and-collectors/#operators Search operators
     * @since 4.7
     */
    public static Bson searchMeta(final SearchOperator operator, final SearchOptions options) {
        return new SearchStage("$searchMeta", notNull("operator", operator), notNull("options", options));
    }

    /**
     * Creates a {@code $searchMeta} pipeline stage supported by MongoDB Atlas.
     * Unlike {@link #search(SearchCollector) $search}, it does not return found documents,
     * instead it returns metadata, which in case of using the {@code $search} stage
     * may be extracted by using {@code $$SEARCH_META} variable, e.g., via {@link Projections#computedSearchMeta(String)}.
     *
     * @param collector A search collector.
     * @return The {@code $searchMeta} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-search/query-syntax/#-searchmeta $searchMeta
     * @mongodb.atlas.manual atlas-search/operators-and-collectors/#collectors Search collectors
     * @since 4.7
     */
    public static Bson searchMeta(final SearchCollector collector) {
        return searchMeta(collector, searchOptions());
    }

    /**
     * Creates a {@code $searchMeta} pipeline stage supported by MongoDB Atlas.
     * Unlike {@link #search(SearchCollector, SearchOptions) $search}, it does not return found documents,
     * instead it returns metadata, which in case of using the {@code $search} stage
     * may be extracted by using {@code $$SEARCH_META} variable, e.g., via {@link Projections#computedSearchMeta(String)}.
     *
     * @param collector A search collector.
     * @param options Optional {@code $search} pipeline stage fields.
     * Specifying {@link SearchOptions#searchOptions()} is equivalent to calling {@link #searchMeta(SearchCollector)}.
     * @return The {@code $searchMeta} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-search/query-syntax/#-searchmeta $searchMeta
     * @mongodb.atlas.manual atlas-search/operators-and-collectors/#collectors Search collectors
     * @since 4.7
     */
    public static Bson searchMeta(final SearchCollector collector, final SearchOptions options) {
        return new SearchStage("$searchMeta", notNull("collector", collector), notNull("options", options));
    }

    /**
     * Creates a {@code $vectorSearch} pipeline stage supported by MongoDB Atlas.
     * You may use the {@code $meta: "vectorSearchScore"} expression, e.g., via {@link Projections#metaVectorSearchScore(String)},
     * to extract the relevance score assigned to each found document.
     *
     * @param queryVector The query vector. The number of dimensions must match that of the {@code index}.
     * @param path The field to be searched.
     * @param index The name of the index to use.
     * @param limit The limit on the number of documents produced by the pipeline stage.
     * @param options Optional {@code $vectorSearch} pipeline stage fields.
     * @return The {@code $vectorSearch} pipeline stage.
     *
     * @mongodb.atlas.manual atlas-vector-search/vector-search-stage/ $vectorSearch
     * @mongodb.atlas.manual atlas-search/scoring/ Scoring
     * @mongodb.server.release 6.0.11
     * @since 4.11
     */
    public static Bson vectorSearch(
            final FieldSearchPath path,
            final Iterable<Double> queryVector,
            final String index,
            final long limit,
            final VectorSearchOptions options) {
        notNull("path", path);
        notNull("queryVector", queryVector);
        notNull("index", index);
        notNull("options", options);
        return new VectorSearchBson(path, queryVector, index, limit, options);
    }

    /**
     * Creates a {@code $vectorSearch} pipeline stage supported by MongoDB Atlas.
     * You may use the {@code $meta: "vectorSearchScore"} expression, e.g., via {@link Projections#metaVectorSearchScore(String)},
     * to extract the relevance score assigned to each found document.
     *
     * @param queryVector The {@linkplain Vector query vector}. The number of dimensions must match that of the {@code index}.
     * @param path        The field to be searched.
     * @param index       The name of the index to use.
     * @param limit       The limit on the number of documents produced by the pipeline stage.
     * @param options     Optional {@code $vectorSearch} pipeline stage fields.
     * @return The {@code $vectorSearch} pipeline stage.
     * @mongodb.atlas.manual atlas-vector-search/vector-search-stage/ $vectorSearch
     * @mongodb.atlas.manual atlas-search/scoring/ Scoring
     * @mongodb.server.release 6.0
     * @see Vector
     * @since 5.3
     */
    public static Bson vectorSearch(
            final FieldSearchPath path,
            final Vector queryVector,
            final String index,
            final long limit,
            final VectorSearchOptions options) {
        notNull("path", path);
        notNull("queryVector", queryVector);
        notNull("index", index);
        notNull("options", options);
        return new VectorSearchBson(path, queryVector, index, limit, options);
    }

    /**
     * Creates an $unset pipeline stage that removes/excludes fields from documents
     *
     * @param fields the fields to exclude. May use dot notation.
     * @return the $unset pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/unset/ $unset
     * @mongodb.server.release 4.2
     * @since 4.8
     */
    public static Bson unset(final String... fields) {
        return unset(asList(fields));
    }

    /**
     * Creates an $unset pipeline stage that removes/excludes fields from documents
     *
     * @param fields the fields to exclude. May use dot notation.
     * @return the $unset pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/unset/ $unset
     * @mongodb.server.release 4.2
     * @since 4.8
     */
    public static Bson unset(final List<String> fields) {
        if (fields.size() == 1) {
            return new BsonDocument("$unset", new BsonString(fields.get(0)));
        }
        BsonArray array = new BsonArray();
        fields.stream().map(BsonString::new).forEach(array::add);
        return new BsonDocument().append("$unset", array);
    }

    /**
     * Creates a $geoNear pipeline stage that outputs documents in order of nearest to farthest from a specified point.
     *
     * @param near The point for which to find the closest documents.
     * @param distanceField The output field that contains the calculated distance.
     *                      To specify a field within an embedded document, use dot notation.
     * @param options {@link GeoNearOptions}
     * @return the $geoNear pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/geoNear/ $geoNear
     * @since 4.8
     */
    public static Bson geoNear(
            final Point near,
            final String distanceField,
            final GeoNearOptions options) {
        notNull("near", near);
        notNull("distanceField", distanceField);
        notNull("options", options);
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
                writer.writeStartDocument();
                writer.writeStartDocument("$geoNear");

                writer.writeName("near");
                BuildersHelper.encodeValue(writer, near, codecRegistry);
                writer.writeName("distanceField");
                BuildersHelper.encodeValue(writer, distanceField, codecRegistry);

                options.toBsonDocument(documentClass, codecRegistry).forEach((optionName, optionValue) -> {
                    writer.writeName(optionName);
                    BuildersHelper.encodeValue(writer, optionValue, codecRegistry);
                });

                writer.writeEndDocument();
                writer.writeEndDocument();
                return writer.getDocument();
            }

            @Override
            public String toString() {
                return "Stage{name='$geoNear'"
                        + ", near=" + near
                        + ", distanceField=" + distanceField
                        + ", options=" + options
                        + '}';
            }
        };
    }

    /**
     * Creates a $geoNear pipeline stage that outputs documents in order of nearest to farthest from a specified point.
     *
     * @param near The point for which to find the closest documents.
     * @param distanceField The output field that contains the calculated distance.
     *                      To specify a field within an embedded document, use dot notation.
     * @return the $geoNear pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/geoNear/ $geoNear
     * @since 4.8
     */
    public static Bson geoNear(
            final Point near,
            final String distanceField) {
        return geoNear(near, distanceField, geoNearOptions());
    }

    /**
     * Creates a $documents pipeline stage.
     *
     * @param documents the documents.
     * @return the $documents pipeline stage.
     * @mongodb.driver.manual reference/operator/aggregation/documents/ $documents
     * @mongodb.server.release 5.1
     * @since 4.9
     */
    public static Bson documents(final List<? extends Bson> documents) {
        notNull("documents", documents);
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
                writer.writeStartDocument();
                writer.writeStartArray("$documents");
                for (Bson bson : documents) {
                    BuildersHelper.encodeValue(writer, bson, codecRegistry);
                }
                writer.writeEndArray();
                writer.writeEndDocument();
                return writer.getDocument();
            }
        };
    }

    static void writeBucketOutput(final CodecRegistry codecRegistry, final BsonDocumentWriter writer,
                                  @Nullable final List<BsonField> output) {
        if (output != null) {
            writer.writeName("output");
            writer.writeStartDocument();
            for (BsonField field : output) {
                writer.writeName(field.getName());
                BuildersHelper.encodeValue(writer, field.getValue(), codecRegistry);
            }
            writer.writeEndDocument();
        }
    }

    private static class SimplePipelineStage implements Bson {
        private final String name;
        private final Bson value;

        SimplePipelineStage(final String name, final Bson value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            return new BsonDocument(name, value.toBsonDocument(documentClass, codecRegistry));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SimplePipelineStage that = (SimplePipelineStage) o;

            if (!Objects.equals(name, that.name)) {
                return false;
            }
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                           + "name='" + name + '\''
                           + ", value=" + value
                           + '}';
        }
    }

    private static final class BucketStage<TExpression, TBoundary> implements Bson {

        private final TExpression groupBy;
        private final List<TBoundary> boundaries;
        private final BucketOptions options;

        BucketStage(final TExpression groupBy, final List<TBoundary> boundaries, final BucketOptions options) {
            notNull("options", options);
            this.groupBy = groupBy;
            this.boundaries = boundaries;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$bucket");

            writer.writeName("groupBy");
            BuildersHelper.encodeValue(writer, groupBy, codecRegistry);

            writer.writeStartArray("boundaries");
            for (TBoundary boundary : boundaries) {
                BuildersHelper.encodeValue(writer, boundary, codecRegistry);
            }
            writer.writeEndArray();
            Object defaultBucket = options.getDefaultBucket();
            if (defaultBucket != null) {
                writer.writeName("default");
                BuildersHelper.encodeValue(writer, defaultBucket, codecRegistry);
            }
            writeBucketOutput(codecRegistry, writer, options.getOutput());
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BucketStage<?, ?> that = (BucketStage<?, ?>) o;

            if (!Objects.equals(groupBy, that.groupBy)) {
                return false;
            }
            if (!Objects.equals(boundaries, that.boundaries)) {
                return false;
            }
            return options.equals(that.options);
        }

        @Override
        public int hashCode() {
            int result = groupBy != null ? groupBy.hashCode() : 0;
            result = 31 * result + (boundaries != null ? boundaries.hashCode() : 0);
            result = 31 * result + options.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$bucket'"
                + ", boundaries=" + boundaries
                + ", groupBy=" + groupBy
                + ", options=" + options
                + '}';
        }
    }

    private static final class BucketAutoStage<TExpression> implements Bson {

        private final TExpression groupBy;
        private final int buckets;
        private final BucketAutoOptions options;

        BucketAutoStage(final TExpression groupBy, final int buckets, final BucketAutoOptions options) {
            notNull("options", options);
            this.groupBy = groupBy;
            this.buckets = buckets;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$bucketAuto");

            writer.writeName("groupBy");
            BuildersHelper.encodeValue(writer, groupBy, codecRegistry);

            writer.writeInt32("buckets", buckets);

            writeBucketOutput(codecRegistry, writer, options.getOutput());

            BucketGranularity granularity = options.getGranularity();
            if (granularity != null) {
                writer.writeString("granularity", granularity.getValue());
            }
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BucketAutoStage<?> that = (BucketAutoStage<?>) o;

            if (buckets != that.buckets) {
                return false;
            }
            if (!Objects.equals(groupBy, that.groupBy)) {
                return false;
            }
            return options.equals(that.options);
        }

        @Override
        public int hashCode() {
            int result = groupBy != null ? groupBy.hashCode() : 0;
            result = 31 * result + buckets;
            result = 31 * result + options.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$bucketAuto'"
                + ", buckets=" + buckets
                + ", groupBy=" + groupBy
                + ", options=" + options
                + '}';
        }
    }

    private static final class LookupStage<TExpression> implements Bson {
        private final String from;
        private final List<Variable<TExpression>> let;
        private final List<? extends Bson> pipeline;
        private final String as;

        private LookupStage(
                @Nullable final String from,
                @Nullable final List<Variable<TExpression>> let,
                final List<? extends Bson> pipeline,
                final String as) {
            this.from = from;
            this.let = let;
            this.pipeline = pipeline;
            this.as = as;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$lookup");

            if (from != null) {
                writer.writeString("from", from);
            }

            if (let != null) {
                writer.writeStartDocument("let");

                for (Variable<?> variable : let) {
                    writer.writeName(variable.getName());
                    BuildersHelper.encodeValue(writer, variable.getValue(), codecRegistry);
                }

                writer.writeEndDocument();
            }

            writer.writeName("pipeline");
            writer.writeStartArray();
            for (Bson stage : pipeline) {
                BuildersHelper.encodeValue(writer, stage, codecRegistry);
            }
            writer.writeEndArray();

            writer.writeString("as", as);

            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LookupStage<?> that = (LookupStage<?>) o;

            if (!Objects.equals(from, that.from)) {
                return false;
            }
            if (!Objects.equals(let, that.let)) {
                return false;
            }
            if (!Objects.equals(pipeline, that.pipeline)) {
                return false;
            }
            return Objects.equals(as, that.as);
        }

        @Override
        public int hashCode() {
            int result = from != null ? from.hashCode() : 0;
            result = 31 * result + (let != null ? let.hashCode() : 0);
            result = 31 * result + (pipeline != null ? pipeline.hashCode() : 0);
            result = 31 * result + (as != null ? as.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                    + "name='$lookup'"
                    + ", from='" + from + '\''
                    + ", let=" + let
                    + ", pipeline=" + pipeline
                    + ", as='" + as + '\''
                    + '}';
        }
    }

    private static final class GraphLookupStage<TExpression> implements Bson {
        private final String from;
        private final TExpression startWith;
        private final String connectFromField;
        private final String connectToField;
        private final String as;
        private final GraphLookupOptions options;

        private GraphLookupStage(final String from, final TExpression startWith, final String connectFromField, final String connectToField,
                                 final String as, final GraphLookupOptions options) {
            this.from = from;
            this.startWith = startWith;
            this.connectFromField = connectFromField;
            this.connectToField = connectToField;
            this.as = as;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$graphLookup");

            writer.writeString("from", from);
            writer.writeName("startWith");
            BuildersHelper.encodeValue(writer, startWith, codecRegistry);

            writer.writeString("connectFromField", connectFromField);
            writer.writeString("connectToField", connectToField);
            writer.writeString("as", as);
            Integer maxDepth = options.getMaxDepth();
            if (maxDepth != null) {
                writer.writeInt32("maxDepth", maxDepth);
            }
            String depthField = options.getDepthField();
            if (depthField != null) {
                writer.writeString("depthField", depthField);
            }
            Bson restrictSearchWithMatch = options.getRestrictSearchWithMatch();
            if (restrictSearchWithMatch != null) {
                writer.writeName("restrictSearchWithMatch");
                BuildersHelper.encodeValue(writer, restrictSearchWithMatch, codecRegistry);
            }

            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GraphLookupStage<?> that = (GraphLookupStage<?>) o;

            if (!Objects.equals(from, that.from)) {
                return false;
            }
            if (!Objects.equals(startWith, that.startWith)) {
                return false;
            }
            if (!Objects.equals(connectFromField, that.connectFromField)) {
                return false;
            }
            if (!Objects.equals(connectToField, that.connectToField)) {
                return false;
            }
            if (!Objects.equals(as, that.as)) {
                return false;
            }
            return Objects.equals(options, that.options);
        }

        @Override
        public int hashCode() {
            int result = from != null ? from.hashCode() : 0;
            result = 31 * result + (startWith != null ? startWith.hashCode() : 0);
            result = 31 * result + (connectFromField != null ? connectFromField.hashCode() : 0);
            result = 31 * result + (connectToField != null ? connectToField.hashCode() : 0);
            result = 31 * result + (as != null ? as.hashCode() : 0);
            result = 31 * result + (options != null ? options.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$graphLookup'"
                + ", as='" + as + '\''
                + ", connectFromField='" + connectFromField + '\''
                + ", connectToField='" + connectToField + '\''
                + ", from='" + from + '\''
                + ", options=" + options
                + ", startWith=" + startWith
                + '}';
        }
    }

    private static class GroupStage<TExpression> implements Bson {
        private final TExpression id;
        private final List<BsonField> fieldAccumulators;

        GroupStage(final TExpression id, final List<BsonField> fieldAccumulators) {
            this.id = id;
            this.fieldAccumulators = fieldAccumulators;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$group");

            writer.writeName("_id");
            BuildersHelper.encodeValue(writer, id, codecRegistry);

            for (BsonField fieldAccumulator : fieldAccumulators) {
                writer.writeName(fieldAccumulator.getName());
                BuildersHelper.encodeValue(writer, fieldAccumulator.getValue(), codecRegistry);
            }

            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GroupStage<?> that = (GroupStage<?>) o;

            if (!Objects.equals(id, that.id)) {
                return false;
            }
            return Objects.equals(fieldAccumulators, that.fieldAccumulators);
        }

        @Override
        public int hashCode() {
            int result = id != null ? id.hashCode() : 0;
            result = 31 * result + (fieldAccumulators != null ? fieldAccumulators.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                           + "name='$group'"
                           + ", id=" + id
                           + ", fieldAccumulators=" + fieldAccumulators
                           + '}';
        }
    }

    private static class SortByCountStage<TExpression> implements Bson {
        private final TExpression filter;

        SortByCountStage(final TExpression filter) {
            this.filter = filter;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeName("$sortByCount");
            BuildersHelper.encodeValue(writer, filter, codecRegistry);

            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SortByCountStage<?> that = (SortByCountStage<?>) o;

            return Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return filter != null ? filter.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$sortByCount'"
                + ", id=" + filter
                + '}';
        }
    }

    private static class FacetStage implements Bson {

        private final List<Facet> facets;
        FacetStage(final List<Facet> facets) {
            this.facets = facets;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName("$facet");
            writer.writeStartDocument();
            for (Facet facet : facets) {
                writer.writeName(facet.getName());
                writer.writeStartArray();
                for (Bson bson : facet.getPipeline()) {
                    BuildersHelper.encodeValue(writer, bson, codecRegistry);
                }
                writer.writeEndArray();
            }
            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FacetStage that = (FacetStage) o;

            return Objects.equals(facets, that.facets);
        }

        @Override
        public int hashCode() {
            return facets != null ? facets.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$facet', "
                + "facets=" + facets + '}';
        }

    }

    private static class FieldsStage implements Bson {
        private final List<Field<?>> fields;
        private final String stageName; //one of $addFields or $set

        FieldsStage(final String stageName, final List<Field<?>> fields) {
            this.stageName = stageName;
            this.fields = notNull("fields", fields);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName(stageName);
            writer.writeStartDocument();
            for (Field<?> field : fields) {
                writer.writeName(field.getName());
                BuildersHelper.encodeValue(writer, field.getValue(), codecRegistry);
            }
            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FieldsStage that = (FieldsStage) o;

            if (!fields.equals(that.fields)) {
                return false;
            }
            return stageName.equals(that.stageName);
        }

        @Override
        public int hashCode() {
            int result = fields.hashCode();
            result = 31 * result + stageName.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='" + stageName + "', "
                + "fields=" + fields
                + '}';
        }
    }

    private static class ReplaceStage<TExpression> implements Bson {
        private final TExpression value;
        private final boolean replaceWith;

        ReplaceStage(final TExpression value) {
            this(value, false);
        }

        ReplaceStage(final TExpression value, final boolean replaceWith) {
            this.value = value;
            this.replaceWith = replaceWith;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();

            if (replaceWith) {
                writer.writeName("$replaceWith");
                BuildersHelper.encodeValue(writer, value, codecRegistry);
            } else {
                writer.writeName("$replaceRoot");
                writer.writeStartDocument();
                writer.writeName("newRoot");
                BuildersHelper.encodeValue(writer, value, codecRegistry);
                writer.writeEndDocument();
            }
            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ReplaceStage<?> that = (ReplaceStage<?>) o;

            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "Stage{"
                + "name='$replaceRoot', "
                + "value=" + value
                + '}';
        }
    }

    private static class MergeStage implements Bson {
        private final BsonValue intoValue;
        private final MergeOptions options;

        MergeStage(final BsonValue intoValue, final MergeOptions options) {
            this.intoValue = intoValue;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeStartDocument("$merge");
            writer.writeName("into");
            if (intoValue.isString()) {
                writer.writeString(intoValue.asString().getValue());
            } else {
                writer.writeStartDocument();
                writer.writeString("db", intoValue.asDocument().getString("db").getValue());
                writer.writeString("coll", intoValue.asDocument().getString("coll").getValue());
                writer.writeEndDocument();
            }
            if (options.getUniqueIdentifier() != null) {
                if (options.getUniqueIdentifier().size() == 1) {
                    writer.writeString("on", options.getUniqueIdentifier().get(0));
                } else {
                    writer.writeStartArray("on");
                    for (String cur : options.getUniqueIdentifier()) {
                        writer.writeString(cur);
                    }
                    writer.writeEndArray();
                }
            }
            if (options.getVariables() != null) {
                writer.writeStartDocument("let");

                for (Variable<?> variable : options.getVariables()) {
                    writer.writeName(variable.getName());
                    BuildersHelper.encodeValue(writer, variable.getValue(), codecRegistry);
                }

                writer.writeEndDocument();
            }

            if (options.getWhenMatched() != null) {
                writer.writeName("whenMatched");
                switch (options.getWhenMatched()) {
                    case REPLACE:
                        writer.writeString("replace");
                        break;
                    case KEEP_EXISTING:
                        writer.writeString("keepExisting");
                        break;
                    case MERGE:
                        writer.writeString("merge");
                        break;
                    case PIPELINE:
                        writer.writeStartArray();
                        for (Bson curStage : options.getWhenMatchedPipeline()) {
                            BuildersHelper.encodeValue(writer, curStage, codecRegistry);
                        }
                        writer.writeEndArray();
                        break;
                    case FAIL:
                        writer.writeString("fail");
                        break;
                    default:
                        throw new UnsupportedOperationException("Unexpected value: " + options.getWhenMatched());
                }
            }
            if (options.getWhenNotMatched() != null) {
                writer.writeName("whenNotMatched");
                switch (options.getWhenNotMatched()) {
                    case INSERT:
                        writer.writeString("insert");
                        break;
                    case DISCARD:
                        writer.writeString("discard");
                        break;
                    case FAIL:
                        writer.writeString("fail");
                        break;
                    default:
                        throw new UnsupportedOperationException("Unexpected value: " + options.getWhenNotMatched());
                }
            }
            writer.writeEndDocument();
            writer.writeEndDocument();
            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MergeStage that = (MergeStage) o;

            if (!intoValue.equals(that.intoValue)) {
                return false;
            }
            if (!options.equals(that.options)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = intoValue.hashCode();
            result = 31 * result + options.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                    + "name='$merge', "
                    + ", into=" + intoValue
                    + ", options=" + options
                    + '}';
        }
    }

    private static final class UnionWithStage implements Bson {
        private final String collection;
        private final List<? extends Bson> pipeline;

        private UnionWithStage(final String collection, final List<? extends Bson> pipeline) {
            this.collection = collection;
            this.pipeline = pipeline;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();

            writer.writeStartDocument("$unionWith");
            writer.writeString("coll", collection);

            writer.writeName("pipeline");
            writer.writeStartArray();
            for (Bson stage : pipeline) {
                BuildersHelper.encodeValue(writer, stage, codecRegistry);
            }
            writer.writeEndArray();

            writer.writeEndDocument();

            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            UnionWithStage that = (UnionWithStage) o;

            if (!collection.equals(that.collection)) {
                return false;
            }
            return !Objects.equals(pipeline, that.pipeline);
        }

        @Override
        public int hashCode() {
            int result = collection.hashCode();
            result = 31 * result + (pipeline != null ? pipeline.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Stage{"
                    + "name='$unionWith'"
                    + ", collection='" + collection + '\''
                    + ", pipeline=" + pipeline
                    + '}';
        }
    }

    private static final class SetWindowFieldsStage<TExpression> implements Bson {
        @Nullable
        private final TExpression partitionBy;
        @Nullable
        private final Bson sortBy;
        private final Iterable<? extends WindowOutputField> output;

        SetWindowFieldsStage(
                @Nullable final TExpression partitionBy,
                @Nullable final Bson sortBy,
                final Iterable<? extends WindowOutputField> output) {
            this.partitionBy = partitionBy;
            this.sortBy = sortBy;
            this.output = output;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeStartDocument("$setWindowFields");
            if (partitionBy != null) {
                writer.writeName("partitionBy");
                BuildersHelper.encodeValue(writer, partitionBy, codecRegistry);
            }
            if (sortBy != null) {
                writer.writeName("sortBy");
                BuildersHelper.encodeValue(writer, sortBy, codecRegistry);
            }
            writer.writeStartDocument("output");
            for (WindowOutputField windowOutputField : output) {
                BsonField field = windowOutputField.toBsonField();
                writer.writeName(field.getName());
                BuildersHelper.encodeValue(writer, field.getValue(), codecRegistry);
            }
            writer.writeEndDocument(); // end output
            writer.writeEndDocument(); // end $setWindowFields
            writer.writeEndDocument();
            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SetWindowFieldsStage<?> that = (SetWindowFieldsStage<?>) o;
            return Objects.equals(partitionBy, that.partitionBy) && Objects.equals(sortBy, that.sortBy) && output.equals(that.output);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionBy, sortBy, output);
        }

        @Override
        public String toString() {
            return "Stage{"
                    + "name='$setWindowFields'"
                    + ", partitionBy=" + partitionBy
                    + ", sortBy=" + sortBy
                    + ", output=" + output
                    + '}';
        }
    }

    private static final class SearchStage implements Bson {
        private final String name;
        private final Bson operatorOrCollector;
        @Nullable
        private final SearchOptions options;

        SearchStage(final String name, final Bson operatorOrCollector, @Nullable final SearchOptions options) {
            this.name = name;
            this.operatorOrCollector = operatorOrCollector;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeStartDocument(name);
            BsonDocument operatorOrCollectorDoc = operatorOrCollector.toBsonDocument(documentClass, codecRegistry);
            assertTrue(operatorOrCollectorDoc.size() == 1);
            Map.Entry<String, BsonValue> operatorOrCollectorEntry = operatorOrCollectorDoc.entrySet().iterator().next();
            writer.writeName(operatorOrCollectorEntry.getKey());
            BuildersHelper.encodeValue(writer, operatorOrCollectorEntry.getValue(), codecRegistry);
            if (options != null) {
                options.toBsonDocument(documentClass, codecRegistry).forEach((optionName, optionValue) -> {
                    writer.writeName(optionName);
                    BuildersHelper.encodeValue(writer, optionValue, codecRegistry);
                });
            }
            // end `name`
            writer.writeEndDocument();
            writer.writeEndDocument();
            return writer.getDocument();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SearchStage that = (SearchStage) o;
            return name.equals(that.name)
                    && operatorOrCollector.equals(that.operatorOrCollector)
                    && Objects.equals(options, that.options);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, operatorOrCollector, options);
        }

        @Override
        public String toString() {
            return "Stage{"
                    + "name='" + name + "'"
                    + ", operatorOrCollector=" + operatorOrCollector
                    + ", options=" + options
                    + '}';
        }
    }

    private static class VectorSearchBson implements Bson {
        private final FieldSearchPath path;
        private final Object queryVector;
        private final String index;
        private final long limit;
        private final VectorSearchOptions options;

        VectorSearchBson(final FieldSearchPath path, final Object queryVector,
                                final String index, final long limit,
                                final VectorSearchOptions options) {
            this.path = path;
            this.queryVector = queryVector;
            this.index = index;
            this.limit = limit;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            Document specificationDoc = new Document("path", path.toValue())
                    .append("queryVector", queryVector)
                    .append("index", index)
                    .append("limit", limit);
            specificationDoc.putAll(options.toBsonDocument(documentClass, codecRegistry));
            return new Document("$vectorSearch", specificationDoc).toBsonDocument(documentClass, codecRegistry);
        }

        @Override
        public String toString() {
            return "Stage{name=$vectorSearch"
                    + ", path=" + path
                    + ", queryVector=" + queryVector
                    + ", index=" + index
                    + ", limit=" + limit
                    + ", options=" + options
                    + '}';
        }
    }

    private Aggregates() {
    }
}
