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
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
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

    public IndexRequest(final BsonDocument keys) {
        this.keys = notNull("keys", keys);
    }

    public BsonDocument getKeys() {
        return keys;
    }

    public boolean isBackground() {
        return background;
    }

    public IndexRequest background(final boolean background) {
        this.background = background;
        return this;
    }

    public boolean isUnique() {
        return unique;
    }

    public IndexRequest unique(final boolean unique) {
        this.unique = unique;
        return this;
    }

    public String getName() {
        return name;
    }

    public IndexRequest name(final String name) {
        this.name = name;
        return this;
    }

    public boolean isSparse() {
        return sparse;
    }

    public IndexRequest sparse(final boolean sparse) {
        this.sparse = sparse;
        return this;
    }

    public Long getExpireAfter(final TimeUnit timeUnit) {
        if (expireAfterSeconds == null) {
            return null;
        }
        return timeUnit.convert(expireAfterSeconds, TimeUnit.SECONDS);
    }

    public IndexRequest expireAfter(final Long expireAfter, final TimeUnit timeUnit) {
        if (expireAfter == null) {
            this.expireAfterSeconds = null;
        } else {
            this.expireAfterSeconds = TimeUnit.SECONDS.convert(expireAfter, timeUnit);
        }
        return this;
    }

    public Integer getVersion() {
        return this.version;
    }

    public IndexRequest version(final Integer version) {
        this.version = version;
        return this;
    }

    public BsonDocument getWeights() {
        return weights;
    }

    public IndexRequest weights(final BsonDocument weights) {
        this.weights = weights;
        return this;
    }

    public String getDefaultLanguage() {
        return defaultLanguage;
    }

    public IndexRequest defaultLanguage(final String defaultLanguage) {
        this.defaultLanguage = defaultLanguage;
        return this;
    }

    public String getLanguageOverride() {
        return languageOverride;
    }

    public IndexRequest languageOverride(final String languageOverride) {
        this.languageOverride = languageOverride;
        return this;
    }

    public Integer getTextVersion() {
        return textVersion;
    }

    public IndexRequest textVersion(final Integer textVersion) {
        if (textVersion != null) {
            isTrueArgument("textVersion must be 1, 2 or 3", VALID_TEXT_INDEX_VERSIONS.contains(textVersion));
        }
        this.textVersion = textVersion;
        return this;
    }

    public Integer getSphereVersion() {
        return sphereVersion;
    }

    public IndexRequest sphereVersion(final Integer sphereVersion) {
        if (sphereVersion != null) {
            isTrueArgument("sphereIndexVersion must be 1, 2 or 3", VALID_SPHERE_INDEX_VERSIONS.contains(sphereVersion));
        }
        this.sphereVersion = sphereVersion;
        return this;
    }

    public Integer getBits() {
        return bits;
    }

    public IndexRequest bits(final Integer bits) {
        this.bits = bits;
        return this;
    }

    public Double getMin() {
        return min;
    }

    public IndexRequest min(final Double min) {
        this.min = min;
        return this;
    }

    public Double getMax() {
        return max;
    }

    public IndexRequest max(final Double max) {
        this.max = max;
        return this;
    }

    @Deprecated
    public Double getBucketSize() {
        return bucketSize;
    }

    @Deprecated
    public IndexRequest bucketSize(final Double bucketSize) {
        this.bucketSize = bucketSize;
        return this;
    }

    public boolean getDropDups() {
        return dropDups;
    }

    public IndexRequest dropDups(final boolean dropDups) {
        this.dropDups = dropDups;
        return this;
    }

    public BsonDocument getStorageEngine() {
        return storageEngine;
    }

    public IndexRequest storageEngine(final BsonDocument storageEngineOptions) {
        this.storageEngine = storageEngineOptions;
        return this;
    }

    public BsonDocument getPartialFilterExpression() {
        return partialFilterExpression;
    }

    public IndexRequest partialFilterExpression(final BsonDocument partialFilterExpression) {
        this.partialFilterExpression = partialFilterExpression;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public IndexRequest collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    public BsonDocument getWildcardProjection() {
        return wildcardProjection;
    }

    public IndexRequest wildcardProjection(final BsonDocument wildcardProjection) {
        this.wildcardProjection = wildcardProjection;
        return this;
    }

    public boolean isHidden() {
        return hidden;
    }

    public IndexRequest hidden(final boolean hidden) {
        this.hidden = hidden;
        return this;
    }
}
