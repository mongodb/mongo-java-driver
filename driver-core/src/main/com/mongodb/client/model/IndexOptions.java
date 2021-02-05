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

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * The options to apply to the creation of an index.
 *
 * @mongodb.driver.manual reference/command/createIndexes Index options
 * @since 3.0
 */
public class IndexOptions {
    private boolean background;
    private boolean unique;
    private String name;
    private boolean sparse;
    private Long expireAfterSeconds;
    private Integer version;
    private Bson weights;
    private String defaultLanguage;
    private String languageOverride;
    private Integer textVersion;
    private Integer sphereVersion;
    private Integer bits;
    private Double min;
    private Double max;
    private Double bucketSize;
    private Bson storageEngine;
    private Bson partialFilterExpression;
    private Collation collation;
    private Bson wildcardProjection;
    private boolean hidden;

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
    public IndexOptions background(final boolean background) {
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
    public IndexOptions unique(final boolean unique) {
        this.unique = unique;
        return this;
    }

    /**
     * Gets the name of the index.
     *
     * @return the name of the index
     */
    @Nullable
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the index.
     *
     * @param name of the index
     * @return this
     */
    public IndexOptions name(@Nullable final String name) {
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
    public IndexOptions sparse(final boolean sparse) {
        this.sparse = sparse;
        return this;
    }

    /**
     * Gets the time to live for documents in the collection
     *
     * @return the time to live for documents in the collection
     * @param timeUnit the time unit
     * @mongodb.driver.manual tutorial/expire-data TTL
     */
    @Nullable
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
     * @param timeUnit the time unit for expireAfter
     * @return this
     * @mongodb.driver.manual tutorial/expire-data TTL
     */
    public IndexOptions expireAfter(@Nullable final Long expireAfter, final TimeUnit timeUnit) {
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
    @Nullable
    public Integer getVersion() {
        return this.version;
    }

    /**
     * Sets the index version number.
     *
     * @param version the index version number
     * @return this
     */
    public IndexOptions version(@Nullable final Integer version) {
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
    @Nullable
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
    public IndexOptions weights(@Nullable final Bson weights) {
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
    @Nullable
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
    public IndexOptions defaultLanguage(@Nullable final String defaultLanguage) {
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
    @Nullable
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
    public IndexOptions languageOverride(@Nullable final String languageOverride) {
        this.languageOverride = languageOverride;
        return this;
    }

    /**
     * The text index version number.
     *
     * @return the text index version number.
     */
    @Nullable
    public Integer getTextVersion() {
        return textVersion;
    }

    /**
     * Set the text index version number.
     *
     * @param textVersion the text index version number.
     * @return this
     */
    public IndexOptions textVersion(@Nullable final Integer textVersion) {
        this.textVersion = textVersion;
        return this;
    }

    /**
     * Gets the 2dsphere index version number.
     *
     * @return the 2dsphere index version number
     */
    @Nullable
    public Integer getSphereVersion() {
        return sphereVersion;
    }

    /**
     * Sets the 2dsphere index version number.
     *
     * @param sphereVersion the 2dsphere index version number.
     * @return this
     */
    public IndexOptions sphereVersion(@Nullable final Integer sphereVersion) {
        this.sphereVersion = sphereVersion;
        return this;
    }

    /**
     * Gets the number of precision of the stored geohash value of the location data in 2d indexes.
     *
     * @return the number of precision of the stored geohash value
     */
    @Nullable
    public Integer getBits() {
        return bits;
    }

    /**
     * Sets the number of precision of the stored geohash value of the location data in 2d indexes.
     *
     * @param bits the number of precision of the stored geohash value
     * @return this
     */
    public IndexOptions bits(@Nullable final Integer bits) {
        this.bits = bits;
        return this;
    }

    /**
     * Gets the lower inclusive boundary for the longitude and latitude values for 2d indexes..
     *
     * @return the lower inclusive boundary for the longitude and latitude values.
     */
    @Nullable
    public Double getMin() {
        return min;
    }

    /**
     * Sets the lower inclusive boundary for the longitude and latitude values for 2d indexes..
     *
     * @param min the lower inclusive boundary for the longitude and latitude values
     * @return this
     */
    public IndexOptions min(@Nullable final Double min) {
        this.min = min;
        return this;
    }

    /**
     * Gets the upper inclusive boundary for the longitude and latitude values for 2d indexes..
     *
     * @return the upper inclusive boundary for the longitude and latitude values.
     */
    @Nullable
    public Double getMax() {
        return max;
    }

    /**
     * Sets the upper inclusive boundary for the longitude and latitude values for 2d indexes..
     *
     * @param max the upper inclusive boundary for the longitude and latitude values
     * @return this
     */
    public IndexOptions max(@Nullable final Double max) {
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
    @Nullable
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
    public IndexOptions bucketSize(@Nullable final Double bucketSize) {
        this.bucketSize = bucketSize;
        return this;
    }

    /**
     * Gets the storage engine options document for this index.
     *
     * @return the storage engine options
     * @mongodb.server.release 3.0
     */
    @Nullable
    public Bson getStorageEngine() {
        return storageEngine;
    }

    /**
     * Sets the storage engine options document for this index.
     *
     * @param storageEngine the storage engine options
     * @return this
     * @mongodb.server.release 3.0
     */
    public IndexOptions storageEngine(@Nullable final Bson storageEngine) {
        this.storageEngine = storageEngine;
        return this;
    }

    /**
     * Get the filter expression for the documents to be included in the index or null if not set
     *
     * @return the filter expression for the documents to be included in the index or null if not set
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    @Nullable
    public Bson getPartialFilterExpression() {
        return partialFilterExpression;
    }

    /**
     * Sets the filter expression for the documents to be included in the index
     *
     * @param partialFilterExpression the filter expression for the documents to be included in the index
     * @return this
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public IndexOptions partialFilterExpression(@Nullable final Bson partialFilterExpression) {
        this.partialFilterExpression = partialFilterExpression;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public IndexOptions collation(@Nullable final Collation collation) {
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
    public Bson getWildcardProjection() {
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
    public IndexOptions wildcardProjection(final Bson wildcardProjection) {
        this.wildcardProjection = wildcardProjection;
        return this;
    }

    /**
     * Gets whether the index should not be used by the query planner when executing operations.
     *
     * @return true if the index should not be used by the query planner when executing operations.
     * @mongodb.server.release 4.4
     * @since 4.1
     */
    public boolean isHidden() {
        return hidden;
    }

    /**
     * Should the index not be used by the query planner when executing operations.
     *
     * @param hidden true if the index should be hidden
     * @return this
     * @mongodb.server.release 4.4
     * @since 4.1
     */
    public IndexOptions hidden(final boolean hidden) {
        this.hidden = hidden;
        return this;
    }

    @Override
    public String toString() {
        return "IndexOptions{"
                + "background=" + background
                + ", unique=" + unique
                + ", name='" + name + '\''
                + ", sparse=" + sparse
                + ", expireAfterSeconds=" + expireAfterSeconds
                + ", version=" + version
                + ", weights=" + weights
                + ", defaultLanguage='" + defaultLanguage + '\''
                + ", languageOverride='" + languageOverride + '\''
                + ", textVersion=" + textVersion
                + ", sphereVersion=" + sphereVersion
                + ", bits=" + bits
                + ", min=" + min
                + ", max=" + max
                + ", bucketSize=" + bucketSize
                + ", storageEngine=" + storageEngine
                + ", partialFilterExpression=" + partialFilterExpression
                + ", collation=" + collation
                + ", wildcardProjection=" + wildcardProjection
                + ", hidden=" + hidden
                + '}';
    }
}
