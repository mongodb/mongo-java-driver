/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static java.util.Arrays.asList;

/**
 * The options to apply to an operation that atomically finds a document and deletes it.
 *
 * @mongodb.driver.manual reference/method/db.collection.ensureIndex/#options Index options
 * @since 3.0
 */
public class CreateIndexOptions {
    private static final List<Integer> VALID_TEXT_INDEX_VERSIONS = asList(1, 2);
    private static final List<Integer> VALID_SPHERE_INDEX_VERSIONS = asList(1, 2);
    private boolean background;
    private boolean unique;
    private String name;
    private boolean sparse;
    private Integer expireAfterSeconds;
    private Integer version;
    private Bson weights;
    private String defaultLanguage;
    private String languageOverride;
    private Integer textIndexVersion;
    private Integer sphereIndexVersion;
    private Integer bits;
    private Double min;
    private Double max;
    private Double bucketSize;

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
    public CreateIndexOptions background(final boolean background) {
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
    public CreateIndexOptions unique(final boolean unique) {
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
    public CreateIndexOptions name(final String name) {
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
    public CreateIndexOptions sparse(final boolean sparse) {
        this.sparse = sparse;
        return this;
    }

    /**
     * Gets the time to live for documents in the collection
     *
     * @return the time to live for documents in the collection
     * @mongodb.driver.manual tutorial/expire-data TTL
     */
    public Integer getExpireAfterSeconds() {
        return expireAfterSeconds;
    }

    /**
     * s the time to live for documents in the collection
     *
     * @param expireAfterSeconds the time to live for documents in the collection
     * @return this
     * @mongodb.driver.manual tutorial/expire-data TTL
     */
    public CreateIndexOptions expireAfterSeconds(final Integer expireAfterSeconds) {
        this.expireAfterSeconds = expireAfterSeconds;
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
    public CreateIndexOptions version(final Integer version) {
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
    public Bson getWeights() {
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
    public CreateIndexOptions weights(final Bson weights) {
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
    public CreateIndexOptions defaultLanguage(final String defaultLanguage) {
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
    public CreateIndexOptions languageOverride(final String languageOverride) {
        this.languageOverride = languageOverride;
        return this;
    }

    /**
     * The text index version number.
     *
     * @return the text index version number.
     */
    public Integer getTextIndexVersion() {
        return textIndexVersion;
    }

    /**
     * Set the text index version number.
     *
     * @param textIndexVersion the text index version number.
     * @return this
     */
    public CreateIndexOptions textIndexVersion(final int textIndexVersion) {
        isTrueArgument("textIndexVersion must be 1 or 2", VALID_TEXT_INDEX_VERSIONS.contains(textIndexVersion));
        this.textIndexVersion = textIndexVersion;
        return this;
    }

    /**
     * Gets the 2dsphere index version number.
     *
     * @return the 2dsphere index version number
     */
    public Integer getTwoDSphereIndexVersion() {
        return sphereIndexVersion;
    }

    /**
     * Sets the 2dsphere index version number.
     *
     * @param sphereIndexVersion the 2dsphere index version number.
     * @return this
     */
    public CreateIndexOptions twoDSphereIndexVersion(final int sphereIndexVersion) {
        isTrueArgument("sphereIndexVersion must be 1 or 2", VALID_SPHERE_INDEX_VERSIONS.contains(sphereIndexVersion));
        this.sphereIndexVersion = sphereIndexVersion;
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
    public CreateIndexOptions bits(final Integer bits) {
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
    public CreateIndexOptions min(final Double min) {
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
    public CreateIndexOptions max(final Double max) {
        this.max = max;
        return this;
    }

    /**
     * Gets the specified the number of units within which to group the location values for geoHaystack Indexes
     *
     * @return the specified the number of units within which to group the location values for geoHaystack Indexes
     * @mongodb.driver.manual core/geohaystack/ geoHaystack Indexes
     */
    public Double getBucketSize() {
        return bucketSize;
    }

    /**
     * Sets the specified the number of units within which to group the location values for geoHaystack Indexes
     *
     * @param bucketSize the specified the number of units within which to group the location values for geoHaystack Indexes
     * @return this
     * @mongodb.driver.manual core/geohaystack/ geoHaystack Indexes
     */
    public CreateIndexOptions bucketSize(final Double bucketSize) {
        this.bucketSize = bucketSize;
        return this;
    }
}
