/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
 * A factory for defining index keys. A convenient way to use this class is to statically import all of its methods, which allows usage
 * like:
 * <blockquote><pre>
 *    collection.createIndex(compoundIndex(ascending("x"), descending("y")));
 * </pre></blockquote>
 * @since 3.1
 */
public final class Indexes {

    private Indexes() {
    }

    /**
     * Create an index key for an ascending index on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the index specification
     * @mongodb.driver.manual core/indexes indexes
     */
    public static Bson ascending(final String... fieldNames) {
        return ascending(asList(fieldNames));
    }

    /**
     * Create an index key for an ascending index on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the index specification
     * @mongodb.driver.manual core/indexes indexes
     */
    public static Bson ascending(final List<String> fieldNames) {
        notNull("fieldNames", fieldNames);
        return compoundIndex(fieldNames, new BsonInt32(1));
    }

    /**
     * Create an index key for an ascending index on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the index specification
     * @mongodb.driver.manual core/indexes indexes
     */
    public static Bson descending(final String... fieldNames) {
        return descending(asList(fieldNames));
    }

    /**
     * Create an index key for an ascending index on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the index specification
     * @mongodb.driver.manual core/indexes indexes
     */
    public static Bson descending(final List<String> fieldNames) {
        notNull("fieldNames", fieldNames);
        return compoundIndex(fieldNames, new BsonInt32(-1));
    }

    /**
     * Create an index key for an 2dsphere index on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the index specification
     * @mongodb.driver.manual core/2dsphere 2dsphere Index
     */
    public static Bson geo2dsphere(final String... fieldNames) {
        return geo2dsphere(asList(fieldNames));
    }

    /**
     * Create an index key for an ascending index on the given fields.
     *
     * @param fieldNames the field names, which must contain at least one
     * @return the index specification
     * @mongodb.driver.manual core/indexes indexes
     */
    public static Bson geo2dsphere(final List<String> fieldNames) {
        notNull("fieldNames", fieldNames);
        return compoundIndex(fieldNames, new BsonString("2dsphere"));
    }

    /**
     * Create an index key for a 2d index on the given field.
     *
     * <p>
     * <strong>Note: </strong>A 2d index is for data stored as points on a two-dimensional plane.
     * The 2d index is intended for legacy coordinate pairs used in MongoDB 2.2 and earlier.
     * </p>
     *
     * @param fieldName the field to create a 2d index on
     * @return the index specification
     * @mongodb.driver.manual core/2d 2d index
     */
    public static Bson geo2d(final String fieldName) {
        notNull("fieldName", fieldName);
        return new BsonDocument(fieldName, new BsonString("2d"));
    }

    /**
     * Create an index key for a geohaystack index on the given field.
     *
     * <p>
     * <strong>Note: </strong>For queries that use spherical geometry, a 2dsphere index is a better option than a haystack index.
     * 2dsphere indexes allow field reordering; geoHaystack indexes require the first field to be the location field. Also, geoHaystack
     * indexes are only usable via commands and so always return all results at once..
     * </p>
     *
     * @param fieldName the field to create a geoHaystack index on
     * @param additional the additional field that forms the geoHaystack index key
     * @return the index specification
     * @mongodb.driver.manual core/geohaystack geoHaystack index
     */
    public static Bson geoHaystack(final String fieldName, final Bson additional) {
        notNull("fieldName", fieldName);
        return compoundIndex(new BsonDocument(fieldName, new BsonString("geoHaystack")), additional);
    }

    /**
     * Create an index key for a text index on the given field.
     *
     * @param fieldName the field to create a text index on
     * @return the index specification
     * @mongodb.driver.manual core/text text index
     */
    public static Bson text(final String fieldName) {
        notNull("fieldName", fieldName);
        return new BsonDocument(fieldName, new BsonString("text"));
    }

    /**
     * Create an index key for a hashed index on the given field.
     *
     * @param fieldName the field to create a hashed index on
     * @return the index specification
     * @mongodb.driver.manual core/hashed hashed index
     */
    public static Bson hashed(final String fieldName) {
        notNull("fieldName", fieldName);
        return new BsonDocument(fieldName, new BsonString("hashed"));
    }

    /**
     * create a compound index specifications.  If any field names are repeated, the last one takes precedence.
     *
     * @param indexes the index specifications
     * @return the compound index specification
     * @mongodb.driver.manual core/index-compound compoundIndex
     */
    public static Bson compoundIndex(final Bson... indexes) {
        return compoundIndex(asList(indexes));
    }

    /**
     * compound multiple index specifications.  If any field names are repeated, the last one takes precedence.
     *
     * @param indexes the index specifications
     * @return the compound index specification
     * @mongodb.driver.manual core/index-compound compoundIndex
     */
    public static Bson compoundIndex(final List<? extends Bson> indexes) {
        notNull("indexes", indexes);
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                BsonDocument compoundIndex = new BsonDocument();
                for (Bson index : indexes) {
                    BsonDocument indexDocument = index.toBsonDocument(documentClass, codecRegistry);
                    for (String key : indexDocument.keySet()) {
                        compoundIndex.append(key, indexDocument.get(key));
                    }
                }
                return compoundIndex;
            }
        };
    }

    private static Bson compoundIndex(final List<String> fieldNames, final BsonValue value) {
        BsonDocument document = new BsonDocument();
        for (String fieldName : fieldNames) {
            document.append(fieldName, value);
        }
        return document;
    }
}
