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
 * A factory for sort specifications.   A convenient way to use this class is to statically import all of its methods, which allows
 * usage like:
 *
 * <blockquote><pre>
 *    collection.find().sort(orderBy(ascending("x", "y"), descending("z")))
 * </pre></blockquote>
 *
 * @since 3.0
 * @mongodb.driver.manual reference/operator/meta/orderby Sort
 */
public final class Sorts {
    private Sorts() {
    }

    /**
     * Create a sort specification for an ascending sort on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the sort specification
     * @mongodb.driver.manual reference/operator/meta/orderby Sort
     */
    public static Bson ascending(final String... fieldNames) {
        return ascending(asList(fieldNames));
    }

    /**
     * Create a sort specification for an ascending sort on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the sort specification
     * @mongodb.driver.manual reference/operator/meta/orderby Sort
     */
    public static Bson ascending(final List<String> fieldNames) {
        notNull("fieldNames", fieldNames);
        return orderBy(fieldNames, new BsonInt32(1));
    }

    /**
     * Create a sort specification for an ascending sort on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the sort specification
     * @mongodb.driver.manual reference/operator/meta/orderby Sort
     */
    public static Bson descending(final String... fieldNames) {
        return descending(asList(fieldNames));
    }

    /**
     * Create a sort specification for an ascending sort on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the sort specification
     * @mongodb.driver.manual reference/operator/meta/orderby Sort
     */
    public static Bson descending(final List<String> fieldNames) {
        notNull("fieldNames", fieldNames);
        return orderBy(fieldNames, new BsonInt32(-1));
    }

    /**
     * Create a sort specification for the text score meta projection on the given field.
     *
     * @param fieldName the field name
     * @return the sort specification
     * @mongodb.driver.manual reference/operator/projection/meta/#sort textScore
     */
    public static Bson metaTextScore(final String fieldName) {
        return new BsonDocument(fieldName, new BsonDocument("$meta", new BsonString("textScore")));
    }

    /**
     * Combine multiple sort specifications.  If any field names are repeated, the last one takes precedence.
     *
     * @param sorts the sort specifications
     * @return the combined sort specification
     */
    public static Bson orderBy(final Bson... sorts) {
        return orderBy(asList(sorts));
    }

    /**
     * Combine multiple sort specifications.  If any field names are repeated, the last one takes precedence.
     *
     * @param sorts the sort specifications
     * @return the combined sort specification
     */
    public static Bson orderBy(final List<? extends Bson> sorts) {
        notNull("sorts", sorts);
        return new CompoundSort(sorts);
    }

    private static Bson orderBy(final List<String> fieldNames, final BsonValue value) {
        BsonDocument document = new BsonDocument();
        for (String fieldName : fieldNames) {
            document.append(fieldName, value);
        }
        return document;
    }

    private static final class CompoundSort implements Bson {
        private final List<? extends Bson> sorts;

        private CompoundSort(final List<? extends Bson> sorts) {
            this.sorts = sorts;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument combinedDocument = new BsonDocument();
            for (Bson sort : sorts) {
                BsonDocument sortDocument = sort.toBsonDocument(documentClass, codecRegistry);
                for (String key : sortDocument.keySet()) {
                    combinedDocument.append(key, sortDocument.get(key));
                }
            }
            return combinedDocument;
        }

        @Override
        public String toString() {
            return "Compound Sort{"
                           + "sorts=" + sorts
                           + '}';
        }
    }
}
