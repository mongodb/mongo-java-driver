/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;

/**
 * Builders for aggregation pipeline stages.
 *
 * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation pipeline
 * @mongodb.server.release 2.2
 * @since 3.1
 */
public final class Aggregates {

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
     * Creates a $skip pipeline stage
     *
     * @param skip the number of documents to skip
     * @return the $skip pipeline stage
     * @mongodb.driver.manual  reference/operator/aggregation/skip/ $skip
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
     * Creates a $group pipeline stage for the specified filter
     *
     * @param <TExpression> the expression type
     * @param id the id expression for the group
     * @param fieldAccumulators zero or more field accumulator pairs
     * @return the $group pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/group/ $group
     * @mongodb.driver.manual meta/aggregation-quick-reference/#aggregation-expressions Expressions
     */
    public static <TExpression> Bson group(final TExpression id, final BsonField... fieldAccumulators) {
        return group(id, Arrays.asList(fieldAccumulators));
    }

    /**
     * Creates a $group pipeline stage for the specified filter
     *
     * @param <TExpression> the expression type
     * @param id the id expression for the group
     * @param fieldAccumulators zero or more field accumulator pairs
     * @return the $group pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/group/ $group
     * @mongodb.driver.manual meta/aggregation-quick-reference/#aggregation-expressions Expressions
     */
    public static <TExpression> Bson group(final TExpression id, final List<BsonField> fieldAccumulators) {
        return new Bson() {
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
        };
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
     * Creates a $out pipeline stage for the specified filter
     *
     * @param collectionName the collection name
     * @return the $out pipeline stage
     * @mongodb.driver.manual reference/operator/aggregation/out/  $out
     * @mongodb.server.release 2.6
     */
    public static Bson out(final String collectionName) {
        return new BsonDocument("$out", new BsonString(collectionName));
    }

    private static class SimplePipelineStage implements Bson {
        private final String name;
        private final Bson value;

        public SimplePipelineStage(final String name, final Bson value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            return new BsonDocument(name, value.toBsonDocument(documentClass, codecRegistry));
        }
    }

    private Aggregates() {
    }
}
