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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * A factory for projections.   A convenient way to use this class is to statically import all of its methods, which allows usage like:
 *
 * <blockquote><pre>
 *    collection.find().projection(fields(include("x", "y"), excludeId()))
 * </pre></blockquote>
 *
 * @mongodb.driver.manual tutorial/project-fields-from-query-results/#limit-fields-to-return-from-a-query Projection
 * @since 3.0
 */
public final class Projections {
    private Projections() {
    }

    /**
     * Creates a projection of a field whose value is computed from the given expression.  Projection with an expression is only supported
     * using the $project aggregation pipeline stage.
     *
     * @param fieldName     the field name
     * @param expression    the expression
     * @param <TExpression> the expression type
     * @return the projection
     * @see Aggregates#project(Bson)
     */
    public static <TExpression> Bson computed(final String fieldName, final TExpression expression) {
        return new SimpleExpression<TExpression>(fieldName, expression);
    }

    /**
     * Creates a projection that includes all of the given fields.
     *
     * @param fieldNames the field names
     * @return the projection
     */
    public static Bson include(final String... fieldNames) {
        return include(asList(fieldNames));
    }

    /**
     * Creates a projection that includes all of the given fields.
     *
     * @param fieldNames the field names
     * @return the projection
     */
    public static Bson include(final List<String> fieldNames) {
        return combine(fieldNames, new BsonInt32(1));
    }

    /**
     * Creates a projection that excludes all of the given fields.
     *
     * @param fieldNames the field names
     * @return the projection
     */
    public static Bson exclude(final String... fieldNames) {
        return exclude(asList(fieldNames));
    }

    /**
     * Creates a projection that excludes all of the given fields.
     *
     * @param fieldNames the field names
     * @return the projection
     */
    public static Bson exclude(final List<String> fieldNames) {
        return combine(fieldNames, new BsonInt32(0));
    }

    /**
     * Creates a projection that excludes the _id field.  This suppresses the automatic inclusion of _id that is the default, even when
     * other fields are explicitly included.
     *
     * @return the projection
     */
    public static Bson excludeId() {
        return new BsonDocument("_id", new BsonInt32(0));
    }

    /**
     * Creates a projection that includes for the given field only the first element of an array that matches the query filter.  This is
     * referred to as the positional $ operator.
     *
     * @param fieldName the field name whose value is the array
     * @return the projection
     * @mongodb.driver.manual reference/operator/projection/positional/#projection Project the first matching element ($ operator)
     */
    public static Bson elemMatch(final String fieldName) {
        return new BsonDocument(fieldName + ".$", new BsonInt32(1));
    }

    /**
     * Creates a projection that includes for the given field only the first element of the array value of that field that matches the given
     * query filter.
     *
     * @param fieldName the field name
     * @param filter    the filter to apply
     * @return the projection
     * @mongodb.driver.manual reference/operator/projection/elemMatch elemMatch
     */
    public static Bson elemMatch(final String fieldName, final Bson filter) {
        return new ElemMatchFilterProjection(fieldName, filter);
    }

    /**
     * Creates a projection to the given field name of the textScore, for use with text queries.
     *
     * @param fieldName the field name
     * @return the projection
     * @mongodb.driver.manual reference/operator/projection/meta/#projection textScore
     */
    public static Bson metaTextScore(final String fieldName) {
        return new BsonDocument(fieldName, new BsonDocument("$meta", new BsonString("textScore")));
    }

    /**
     * Creates a projection to the given field name of a slice of the array value of that field.
     *
     * @param fieldName the field name
     * @param limit     the number of elements to project.
     * @return the projection
     * @mongodb.driver.manual reference/operator/projection/slice Slice
     */
    public static Bson slice(final String fieldName, final int limit) {
        return new BsonDocument(fieldName, new BsonDocument("$slice", new BsonInt32(limit)));
    }

    /**
     * Creates a projection to the given field name of a slice of the array value of that field.
     *
     * @param fieldName the field name
     * @param skip      the number of elements to skip before applying the limit
     * @param limit     the number of elements to project
     * @return the projection
     * @mongodb.driver.manual reference/operator/projection/slice Slice
     */
    public static Bson slice(final String fieldName, final int skip, final int limit) {
        return new BsonDocument(fieldName, new BsonDocument("$slice", new BsonArray(asList(new BsonInt32(skip), new BsonInt32(limit)))));
    }

    /**
     * Creates a projection that combines the list of projections into a single one.  If there are duplicate keys, the last one takes
     * precedence.
     *
     * @param projections the list of projections to combine
     * @return the combined projection
     */
    public static Bson fields(final Bson... projections) {
        return fields(asList(projections));
    }

    /**
     * Creates a projection that combines the list of projections into a single one.  If there are duplicate keys, the last one takes
     * precedence.
     *
     * @param projections the list of projections to combine
     * @return the combined projection
     */
    public static Bson fields(final List<? extends Bson> projections) {
        notNull("projections", projections);
        return new FieldsProjection(projections);
    }

    private static class FieldsProjection implements Bson {
        private final List<? extends Bson> projections;

        FieldsProjection(final List<? extends Bson> projections) {
            this.projections = projections;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument combinedDocument = new BsonDocument();
            for (Bson sort : projections) {
                BsonDocument sortDocument = sort.toBsonDocument(documentClass, codecRegistry);
                for (String key : sortDocument.keySet()) {
                    combinedDocument.remove(key);
                    combinedDocument.append(key, sortDocument.get(key));
                }
            }
            return combinedDocument;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FieldsProjection that = (FieldsProjection) o;

            return projections != null ? projections.equals(that.projections) : that.projections == null;
        }

        @Override
        public int hashCode() {
            return projections != null ? projections.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "Projections{"
                           + "projections=" + projections
                           + '}';
        }
    }


    private static class ElemMatchFilterProjection implements Bson {
        private final String fieldName;
        private final Bson filter;

        ElemMatchFilterProjection(final String fieldName, final Bson filter) {
            this.fieldName = fieldName;
            this.filter = filter;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            return new BsonDocument(fieldName, new BsonDocument("$elemMatch", filter.toBsonDocument(documentClass, codecRegistry)));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ElemMatchFilterProjection that = (ElemMatchFilterProjection) o;

            if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
                return false;
            }
            return filter != null ? filter.equals(that.filter) : that.filter == null;
        }

        @Override
        public int hashCode() {
            int result = fieldName != null ? fieldName.hashCode() : 0;
            result = 31 * result + (filter != null ? filter.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ElemMatch Projection{"
                           + "fieldName='" + fieldName + '\''
                           + ", filter=" + filter
                           + '}';
        }
    }

    private static Bson combine(final List<String> fieldNames, final BsonValue value) {
        BsonDocument document = new BsonDocument();
        for (String fieldName : fieldNames) {
            document.remove(fieldName);
            document.append(fieldName, value);
        }
        return document;
    }
}
