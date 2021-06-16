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

package com.mongodb.internal.bulk;

import com.mongodb.client.model.Collation;
import org.bson.BsonDocument;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * The settings to apply to the creation of an index.
 *
 * @mongodb.driver.manual reference/method/db.collection.ensureIndex/#options Index options
 * @since 3.0
 */
public class IndexRequest {
    private final BsonDocument keys;
    private static final List<Integer> VALID_TEXT_INDEX_VERSIONS = asList(1, 2, 3);
    private static final List<Integer> VALID_SPHERE_INDEX_VERSIONS = asList(1, 2, 3);
    private boolean background;
    private boolean unique;
    private String name;
    private boolean sparse;
    private Long expireAfterSeconds;
    private Integer version;
    private BsonDocument weights;
    private String defaultLanguage;
    private String languageOverride;
    private Integer textVersion;
    private Integer sphereVersion;
    private Integer bits;
    private Double min;
    private Double max;
    private Double bucketSize;
    private boolean dropDups;
    private BsonDocument storageEngine;
    private BsonDocument partialFilterExpression;
    private Collation collation;
    private BsonDocument wildcardProjection;
    private boolean hidden;

    /**
     * Construct a new instance with the given keys
     * @param keys the index keys
     */
    public IndexRequest(final BsonDocument keys) {
        this.keys = notNull("keys", keys);
    }

    /**
     * Gets the index keys
     * @return the index keys
     */
    public BsonDocument getKeys() {
        return keys;
    }

    /**
     * Create the index in the background
     *
     * @return true if should create the index in the background
     */
    public boolean isBackground() {
        return background;
    }

    /**
     * Should the index should be created in the background
     *
     * @param background true if should create the index in the background
     * @return this
     */
    public IndexRequest background(final boolean background) {
        this.background = background;
        return this;
    }

    /**
     * Gets if the index should be unique.
     *
     * @return true if the index should be unique
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Should the index should be unique.
     *
     * @param unique if the index should be unique
     * @return this
     */
    public IndexRequest unique(final boolean unique) {
        this.unique = unique;
        return this;
    }

    /**
     * Gets the name of the index.
     *
     * @return the name of the index
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the index.
     *
     * @param name of the index
     * @return this
     */
    public IndexRequest name(final String name) {
        this.name = name;
        return this;
    }

    /**
     * If true, the index only references documents with the specified field
     *
     * @return if the index should only reference documents with the specified field
     */
    public boolean isSparse() {
        return sparse;
    }

    /**
     * Should the index only references documents with the specified field
     *
     * @param sparse if true, the index only references documents with the specified field
     * @return this
     */
    public IndexRequest sparse(final boolean sparse) {
        this.sparse = sparse;
        return this;
    }

    /**
     * Gets the time to live for documents in the collection
     *
     * @param timeUnit the time unit
     * @return the time to live for documents in the collection
     * @mongodb.driver.manual tutorial/expire-data TTL
     */
    public Long getExpireAfter(final TimeUnit timeUnit) {
        if (expireAfterSeconds == null) {
            return null;
        }
        return timeUnit.convert(expireAfterSeconds, TimeUnit.SECONDS);
    }

    /**
     * Sets the time to live for documents in the collection
     *
     * @param expireAfter the time to live for documents in the collection
     * @param timeUnit the time unit
     * @return this
     * @mongodb.driver.manual tutorial/expire-data TTL
     */
    public IndexRequest expireAfter(final Long expireAfter, final TimeUnit timeUnit) {
        if (expireAfter == null) {
            this.expireAfterSeconds = null;
        } else {
            this.expireAfterSeconds = TimeUnit.SECONDS.convert(expireAfter, timeUnit);
        }
        return this;
    }

    /**
     * Gets the index version number.
     *
     * @return the index version number
     */
    public Integer getVersion() {
        return this.version;
    }

    /**
     * Sets the index version number.
     *
     * @param version the index version number
     * @return this
     */
    public IndexRequest version(final Integer version) {
        this.version = version;
        return this;
    }

    /**
     * Gets the weighting object for use with a text index
     *
     * <p>A document that represents field and weight pairs. The weight is an integer ranging from 1 to 99,999 and denotes the significance
     * of the field relative to the other indexed fields in terms of the score.</p>
     *
     * @return the weighting object
     * @mongodb.driver.manual tutorial/control-results-of-text-search Control Search Results with Weights
     */
    public BsonDocument getWeights() {
        return weights;
    }

    /**
     * Sets the weighting object for use with a text index.
     *
     * <p>An document that represents field and weight pairs. The weight is an integer ranging from 1 to 99,999 and denotes the significance
     * of the field relative to the other indexed fields in terms of the score.</p>
     *
     * @param weights the weighting object
     * @return this
     * @mongodb.driver.manual tutorial/control-results-of-text-search Control Search Results with Weights
     */
    public IndexRequest weights(final BsonDocument weights) {
        this.weights = weights;
        return this;
    }

    /**
     * Gets the language for a text index.
     *
     * <p>The language that determines the list of stop words and the rules for the stemmer and tokenizer.</p>
     *
     * @return the language for a text index.
     * @mongodb.driver.manual reference/text-search-languages Text Search languages
     */
    public String getDefaultLanguage() {
        return defaultLanguage;
    }

    /**
     * Sets the language for the text index.
     *
     * <p>The language that determines the list of stop words and the rules for the stemmer and tokenizer.</p>
     *
     * @param defaultLanguage the language for the text index.
     * @return this
     * @mongodb.driver.manual reference/text-search-languages Text Search languages
     */
    public IndexRequest defaultLanguage(final String defaultLanguage) {
        this.defaultLanguage = defaultLanguage;
        return this;
    }

    /**
     * Gets the name of the field that contains the language string.
     *
     * <p>For text indexes, the name of the field, in the collection's documents, that contains the override language for the document.</p>
     *
     * @return the name of the field that contains the language string.
     * @mongodb.driver.manual tutorial/specify-language-for-text-index/#specify-language-field-text-index-example Language override
     */
    public String getLanguageOverride() {
        return languageOverride;
    }

    /**
     * Sets the name of the field that contains the language string.
     *
     * <p>For text indexes, the name of the field, in the collection's documents, that contains the override language for the document.</p>
     *
     * @param languageOverride the name of the field that contains the language string.
     * @return this
     * @mongodb.driver.manual tutorial/specify-language-for-text-index/#specify-language-field-text-index-example Language override
     */
    public IndexRequest languageOverride(final String languageOverride) {
        this.languageOverride = languageOverride;
        return this;
    }

    /**
     * The text index version number.
     *
     * @return the text index version number.
     */
    public Integer getTextVersion() {
        return textVersion;
    }

    /**
     * Set the text index version number.
     *
     * @param textVersion the text index version number.
     * @return this
     */
    public IndexRequest textVersion(final Integer textVersion) {
        if (textVersion != null) {
            isTrueArgument("textVersion must be 1, 2 or 3", VALID_TEXT_INDEX_VERSIONS.contains(textVersion));
        }
        this.textVersion = textVersion;
        return this;
    }

    /**
     * Gets the 2dsphere index version number.
     *
     * @return the 2dsphere index version number
     */
    public Integer getSphereVersion() {
        return sphereVersion;
    }

    /**
     * Sets the 2dsphere index version number.
     *
     * @param sphereVersion the 2dsphere index version number.
     * @return this
     */
    public IndexRequest sphereVersion(final Integer sphereVersion) {
        if (sphereVersion != null) {
            isTrueArgument("sphereIndexVersion must be 1, 2 or 3", VALID_SPHERE_INDEX_VERSIONS.contains(sphereVersion));
        }
        this.sphereVersion = sphereVersion;
        return this;
    }

    /**
     * Gets the number of precision of the stored geohash value of the location data in 2d indexes.
     *
     * @return the number of precision of the stored geohash value
     */
    public Integer getBits() {
        return bits;
    }

    /**
     * Sets the number of precision of the stored geohash value of the location data in 2d indexes.
     *
     * @param bits the number of precision of the stored geohash value
     * @return this
     */
    public IndexRequest bits(final Integer bits) {
        this.bits = bits;
        return this;
    }

    /**
     * Gets the lower inclusive boundary for the longitude and latitude values for 2d indexes..
     *
     * @return the lower inclusive boundary for the longitude and latitude values.
     */
    public Double getMin() {
        return min;
    }

    /**
     * Sets the lower inclusive boundary for the longitude and latitude values for 2d indexes..
     *
     * @param min the lower inclusive boundary for the longitude and latitude values
     * @return this
     */
    public IndexRequest min(final Double min) {
        this.min = min;
        return this;
    }

    /**
     * Gets the upper inclusive boundary for the longitude and latitude values for 2d indexes..
     *
     * @return the upper inclusive boundary for the longitude and latitude values.
     */
    public Double getMax() {
        return max;
    }

    /**
     * Sets the upper inclusive boundary for the longitude and latitude values for 2d indexes..
     *
     * @param max the upper inclusive boundary for the longitude and latitude values
     * @return this
     */
    public IndexRequest max(final Double max) {
        this.max = max;
        return this;
    }

    /**
     * Gets the specified the number of units within which to group the location values for geoHaystack Indexes
     *
     * @return the specified the number of units within which to group the location values for geoHaystack Indexes
     * @mongodb.driver.manual core/geohaystack/ geoHaystack Indexes
     * @deprecated geoHaystack is deprecated in MongoDB 4.4
     */
    @Deprecated
    public Double getBucketSize() {
        return bucketSize;
    }

    /**
     * Sets the specified the number of units within which to group the location values for geoHaystack Indexes
     *
     * @param bucketSize the specified the number of units within which to group the location values for geoHaystack Indexes
     * @return this
     * @mongodb.driver.manual core/geohaystack/ geoHaystack Indexes
     * @deprecated geoHaystack is deprecated in MongoDB 4.4
     */
    @Deprecated
    public IndexRequest bucketSize(final Double bucketSize) {
        this.bucketSize = bucketSize;
        return this;
    }

    /**
     * Returns the legacy dropDups setting
     *
     * <p>Prior to MongoDB 3.0 dropDups could be used with unique indexes allowing documents with duplicate values to be dropped when
     * building the index. Later versions of MongoDB will silently ignore this setting.</p>
     *
     * @return the legacy dropDups setting
     * @mongodb.driver.manual core/index-creation/#index-creation-duplicate-dropping duplicate dropping
     */
    public boolean getDropDups() {
        return dropDups;
    }

    /**
     * Sets the legacy dropDups setting
     *
     * <p>Prior to MongoDB 3.0 dropDups could be used with unique indexes allowing documents with duplicate values to be dropped when
     * building the index. Later versions of MongoDB will silently ignore this setting.</p>
     *
     * @param dropDups the legacy dropDups setting
     * @return this
     * @mongodb.driver.manual core/index-creation/#index-creation-duplicate-dropping duplicate dropping
     */
    public IndexRequest dropDups(final boolean dropDups) {
        this.dropDups = dropDups;
        return this;
    }

    /**
     * Gets the storage engine options document for this index.
     *
     * @return the storage engine options
     * @mongodb.server.release 3.0
     */
    public BsonDocument getStorageEngine() {
        return storageEngine;
    }

    /**
     * Sets the storage engine options document for this index.
     *
     * @param storageEngineOptions the storage engine options
     * @return this
     * @mongodb.server.release 3.0
     */
    public IndexRequest storageEngine(final BsonDocument storageEngineOptions) {
        this.storageEngine = storageEngineOptions;
        return this;
    }

    /**
     * Get the filter expression for the documents to be included in the index or null if not set
     *
     * @return the filter expression for the documents to be included in the index or null if not set
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual /core/index-partial/ Partial Indexes
     * @since 3.2
     */
    public BsonDocument getPartialFilterExpression() {
        return partialFilterExpression;
    }

    /**
     * Sets the filter expression for the documents to be included in the index
     *
     * @param partialFilterExpression the filter expression for the documents to be included in the index
     * @return this
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual /core/index-partial/ Partial Indexes
     * @since 3.2
     */
    public IndexRequest partialFilterExpression(final BsonDocument partialFilterExpression) {
        this.partialFilterExpression = partialFilterExpression;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public IndexRequest collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Gets the wildcard projection of a wildcard index
     *
     * @return the wildcard projection
     * @mongodb.server.release 4.2
     * @since 3.10
     */
    public BsonDocument getWildcardProjection() {
        return wildcardProjection;
    }

    /**
     * Sets the wildcard projection of a wildcard index
     *
     * @param wildcardProjection the wildcard projection
     * @return this
     * @mongodb.server.release 4.2
     * @since 3.10
     */
    public IndexRequest wildcardProjection(final BsonDocument wildcardProjection) {
        this.wildcardProjection = wildcardProjection;
        return this;
    }

    /**
     * Gets if the index should exist on the target collection but should not be used by the query
     * planner when executing operations.
     *
     * @return true if the index should not be used by the query planner when executing operations.
     */
    public boolean isHidden() {
        return hidden;
    }

    /**
     * Should the index exist on the target collection but should not be used by the query
     * planner when executing operations.
     *
     * @param hidden true if the index should be hidden
     * @return this
     */
    public IndexRequest hidden(final boolean hidden) {
        this.hidden = hidden;
        return this;
    }
}
