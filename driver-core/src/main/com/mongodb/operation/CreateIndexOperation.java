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

package com.mongodb.operation;

import com.mongodb.CommandFailureException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.AcknowledgedWriteConcernResult;
import com.mongodb.protocol.InsertProtocol;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.DUPLICATE_KEY_ERROR_CODES;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotSix;
import static com.mongodb.operation.OperationHelper.withConnection;
import static java.util.Arrays.asList;

/**
 * An operation that creates an index.
 *
 * @mongodb.driver.manual reference/method/db.collection.ensureIndex/#options Index options
 * @since 3.0
 */
public class CreateIndexOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final BsonDocument key;
    private final MongoNamespace systemIndexes;
    private boolean background;
    private boolean unique;
    private String name;
    private boolean sparse;
    private Integer expireAfterSeconds;
    private Integer version;
    private BsonDocument weights;
    private String defaultLanguage;
    private String languageOverride;
    private Integer textIndexVersion;
    private Integer sphereIndexVersion;
    private Integer bits;
    private Double min;
    private Double max;
    private Double bucketSize;
    private boolean dropDups;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param key       the index key.
     */
    public CreateIndexOperation(final MongoNamespace namespace, final BsonDocument key) {
        this.namespace = notNull("namespace", namespace);
        this.systemIndexes = new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
        this.key = notNull("key", key);
    }

    /**
     * Gets the index key.
     *
     * @return the index key.
     */
    public BsonDocument getKey() {
        return key;
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
     * s if the index should be created in the background
     *
     * @param background true if should create the index in the background
     * @return this
     */
    public CreateIndexOperation background(final boolean background) {
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
     * s if the index should be unique.
     *
     * @param unique if the index should be unique
     * @return this
     */
    public CreateIndexOperation unique(final boolean unique) {
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
    public CreateIndexOperation name(final String name) {
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
    public CreateIndexOperation sparse(final boolean sparse) {
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
    public CreateIndexOperation expireAfterSeconds(final Integer expireAfterSeconds) {
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
    public CreateIndexOperation version(final Integer version) {
        this.version = version;
        return this;
    }

    /**
     * Gets the weighting document for use with a text index
     *
     * <p>A document that represents field and weight pairs. The weight is an integer ranging from 1 to 99,999 and denotes the significance
     * of the field relative to the other indexed fields in terms of the score.</p>
     *
     * @return the weighting document
     * @mongodb.driver.manual tutorial/control-results-of-text-search Control Search Results with Weights
     */
    public BsonDocument getWeights() {
        return weights;
    }

    /**
     * Sets the weighting document for use with a text index.
     *
     * <p>A document that represents field and weight pairs. The weight is an integer ranging from 1 to 99,999 and denotes the significance
     * of the field relative to the other indexed fields in terms of the score.</p>
     *
     * @param weights the weighting document
     * @return this
     * @mongodb.driver.manual tutorial/control-results-of-text-search Control Search Results with Weights
     */
    public CreateIndexOperation weights(final BsonDocument weights) {
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
    public CreateIndexOperation defaultLanguage(final String defaultLanguage) {
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
    public CreateIndexOperation languageOverride(final String languageOverride) {
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
    public CreateIndexOperation textIndexVersion(final Integer textIndexVersion) {
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
    public CreateIndexOperation twoDSphereIndexVersion(final Integer sphereIndexVersion) {
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
    public CreateIndexOperation bits(final Integer bits) {
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
    public CreateIndexOperation min(final Double min) {
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
    public CreateIndexOperation max(final Double max) {
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
    public CreateIndexOperation bucketSize(final Double bucketSize) {
        this.bucketSize = bucketSize;
        return this;
    }

    /**
     * Returns the legacy dropDups setting
     *
     * <p>Prior to MongoDB 2.8 dropDups could be used with unique indexes allowing documents with duplicate values to be dropped when
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
     * <p>Prior to MongoDB 2.8 dropDups could be used with unique indexes allowing documents with duplicate values to be dropped when
     * building the index. Later versions of MongoDB will silently ignore this setting.</p>
     *
     * @param dropDups the legacy dropDups setting
     * @return this
     * @mongodb.driver.manual core/index-creation/#index-creation-duplicate-dropping duplicate dropping
     */
    public CreateIndexOperation dropDups(final boolean dropDups) {
        this.dropDups = dropDups;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    try {
                        executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), connection);
                    } catch (CommandFailureException e) {
                        throw checkForDuplicateKeyError(e);
                    }
                } else {
                    asInsertProtocol(getIndex()).execute(connection);
                }
                return null;
            }
        });
    }

    @Override
    public MongoFuture<Void> executeAsync(final AsyncWriteBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnection<Void>() {
            @Override
            public MongoFuture<Void> call(final Connection connection) {
                final SingleResultFuture<Void> future = new SingleResultFuture<Void>();
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), connection)
                        .register(new SingleResultCallback<BsonDocument>() {
                            @Override
                            public void onResult(final BsonDocument result, final MongoException e) {
                                future.init(null, translateException(e));
                            }
                        });
                } else {
                    asInsertProtocol(getIndex()).executeAsync(connection).register(new SingleResultCallback<WriteConcernResult>() {
                        @Override
                        public void onResult(final WriteConcernResult result, final MongoException e) {
                            future.init(null, translateException(e));
                        }
                    });
                }
                return future;
            }
        });
    }

    private BsonDocument getIndex() {
        BsonDocument index = new BsonDocument();
        index.append("key", key);
        index.append("name", new BsonString(getName() != null ? getName() : generateIndexName(key)));
        index.append("ns", new BsonString(namespace.getFullName()));
        if (background) {
            index.append("background", BsonBoolean.TRUE);
        }
        if (unique) {
            index.append("unique", BsonBoolean.TRUE);
        }
        if (sparse) {
            index.append("sparse", BsonBoolean.TRUE);
        }
        if (expireAfterSeconds != null) {
            index.append("expireAfterSeconds", new BsonInt32(expireAfterSeconds));
        }
        if (version != null) {
            index.append("v", new BsonInt32(version));
        }
        if (weights != null) {
            index.append("weights", weights);
        }
        if (defaultLanguage != null) {
            index.append("default_language", new BsonString(defaultLanguage));
        }
        if (languageOverride != null) {
            index.append("language_override", new BsonString(languageOverride));
        }
        if (textIndexVersion != null) {
            index.append("textIndexVersion", new BsonInt32(textIndexVersion));
        }
        if (sphereIndexVersion != null) {
            index.append("2dsphereIndexVersion", new BsonInt32(sphereIndexVersion));
        }
        if (bits != null) {
            index.append("bits", new BsonInt32(bits));
        }
        if (min != null) {
            index.append("min", new BsonDouble(min));
        }
        if (max != null) {
            index.append("max", new BsonDouble(max));
        }
        if (bucketSize != null) {
            index.append("bucketSize", new BsonDouble(bucketSize));
        }
        if (dropDups) {
            index.append("dropDups", BsonBoolean.TRUE);
        }
        return index;
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("createIndexes", new BsonString(namespace.getCollectionName()));
        BsonArray array = new BsonArray(asList(getIndex()));
        command.put("indexes", array);
        return command;
    }

    @SuppressWarnings("unchecked")
    private InsertProtocol asInsertProtocol(final BsonDocument index) {
        return new InsertProtocol(systemIndexes, true, WriteConcern.ACKNOWLEDGED, asList(new InsertRequest(index)));
    }

    private MongoException translateException(final MongoException e) {
        return (e instanceof CommandFailureException) ? checkForDuplicateKeyError((CommandFailureException) e) : e;
    }

    @SuppressWarnings("deprecation")
    private MongoException checkForDuplicateKeyError(final CommandFailureException e) {
        if (DUPLICATE_KEY_ERROR_CODES.contains(e.getCode())) {
            return new MongoException.DuplicateKey(e.getResponse(), e.getServerAddress(),
                                                   new AcknowledgedWriteConcernResult(0, false, null));
        } else {
            return e;
        }
    }

    /**
     * Convenience method to generate an index name from the set of fields it is over.
     *
     * @return a string representation of this index's fields
     */
    private String generateIndexName(final BsonDocument index) {
        StringBuilder indexName = new StringBuilder();
        for (final String keyNames : index.keySet()) {
            if (indexName.length() != 0) {
                indexName.append('_');
            }
            indexName.append(keyNames).append('_');
            BsonValue ascOrDescValue = index.get(keyNames);
            if (ascOrDescValue instanceof BsonInt32) {
                indexName.append(((BsonInt32) ascOrDescValue).getValue());
            } else if (ascOrDescValue instanceof BsonString) {
                indexName.append(((BsonString) ascOrDescValue).getValue().replace(' ', '_'));
            }
        }
        return indexName.toString();
    }

}
