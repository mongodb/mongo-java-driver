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

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A factory for defining fields within a vector search index definition.
 *
 * <p>A convenient way to use this class is to statically import all of its methods, which allows usage like:</p>
 * <pre>{@code
 *    SearchIndexDefinition.vectorSearch(
 *        vectorField("plot_embedding")
 *            .numDimensions(1536)
 *            .similarity("euclidean")
 *            .indexingMethod("flat"),
 *        filterField("genre")
 *    );
 * }</pre>
 *
 * @see SearchIndexDefinition#vectorSearch(Bson...)
 * @since 5.8
 */
public final class VectorSearchIndexFields {

    private VectorSearchIndexFields() {
    }

    /**
     * Creates a vector field definition for a vector search index.
     *
     * @param path the field path in the document
     * @return a new {@link VectorField}
     * @since 5.8
     */
    public static VectorField vectorField(final String path) {
        return new VectorField(notNull("path", path));
    }

    /**
     * Creates a filter field definition for a vector search index.
     *
     * @param path the field path in the document
     * @return a new {@link FilterField}
     * @since 5.8
     */
    public static FilterField filterField(final String path) {
        return new FilterField(notNull("path", path));
    }

    /**
     * Creates an auto-embed field definition for a vector search index.
     *
     * @param path the field path in the document containing the content to embed
     * @return a new {@link AutoEmbedField}
     * @since 5.8
     */
    public static AutoEmbedField autoEmbedField(final String path) {
        return new AutoEmbedField(notNull("path", path));
    }

    /**
     * A vector field definition for a vector search index.
     *
     * <p>Instances are created via {@link #vectorField(String)}.</p>
     *
     * @since 5.8
     */
    @NotThreadSafe
    public static final class VectorField implements Bson {
        private final String path;
        @Nullable
        private Integer numDimensions;
        @Nullable
        private String similarity;
        @Nullable
        private String indexingMethod;
        @Nullable
        private Bson hnswOptions;

        private VectorField(final String path) {
            this.path = path;
        }

        /**
         * Sets the number of dimensions for the vector field.
         *
         * @param numDimensions the number of vector dimensions
         * @return this
         * @since 5.8
         */
        public VectorField numDimensions(final int numDimensions) {
            isTrueArgument("numDimensions > 0", numDimensions > 0);
            this.numDimensions = numDimensions;
            return this;
        }

        /**
         * Sets the similarity function used to compare vectors.
         *
         * <p>Supported values:</p>
         * <ul>
         *   <li>{@code "euclidean"} — measures the distance between ends of vectors</li>
         *   <li>{@code "cosine"} — measures the angle between vectors</li>
         *   <li>{@code "dotProduct"} — measures both the magnitude and direction of vectors</li>
         * </ul>
         *
         * @param similarity the similarity function name
         * @return this
         * @since 5.8
         */
        public VectorField similarity(final String similarity) {
            this.similarity = notNull("similarity", similarity);
            return this;
        }

        /**
         * Sets the indexing method for this vector field.
         *
         * <p>Supported values:</p>
         * <ul>
         *   <li>{@code "flat"} — optimized for multi-tenant use cases with singular, static filters</li>
         *   <li>{@code "hnsw"} — Hierarchical Navigable Small World graph</li>
         * </ul>
         *
         * @param indexingMethod the indexing method name
         * @return this
         * @since 5.8
         */
        public VectorField indexingMethod(final String indexingMethod) {
            this.indexingMethod = notNull("indexingMethod", indexingMethod);
            return this;
        }

        /**
         * Sets the HNSW options for this vector field.
         *
         * <p>This is only applicable when the indexing method is {@code "hnsw"}.
         * A convenience builder is available via {@link HnswSearchIndexOptions}, or a raw
         * {@link org.bson.Document} may be passed directly.</p>
         *
         * @param hnswOptions the HNSW options
         * @return this
         * @see HnswSearchIndexOptions
         * @since 5.8
         */
        public VectorField hnswOptions(final Bson hnswOptions) {
            this.hnswOptions = notNull("hnswOptions", hnswOptions);
            return this;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument doc = new BsonDocument();
            doc.append("type", new BsonString("vector"));
            doc.append("path", new BsonString(path));
            if (numDimensions != null) {
                doc.append("numDimensions", new BsonInt32(numDimensions));
            }
            if (similarity != null) {
                doc.append("similarity", new BsonString(similarity));
            }
            if (indexingMethod != null) {
                doc.append("indexingMethod", new BsonString(indexingMethod));
            }
            if (hnswOptions != null) {
                doc.append("hnswOptions", hnswOptions.toBsonDocument(documentClass, codecRegistry));
            }
            return doc;
        }

        @Override
        public String toString() {
            return "VectorField{"
                    + "path='" + path + '\''
                    + ", numDimensions=" + numDimensions
                    + ", similarity='" + similarity + '\''
                    + ", indexingMethod='" + indexingMethod + '\''
                    + ", hnswOptions=" + hnswOptions
                    + '}';
        }
    }

    /**
     * A filter field definition for a vector search index.
     *
     * <p>Instances are created via {@link #filterField(String)}.</p>
     *
     * @since 5.8
     */
    public static final class FilterField implements Bson {
        private final String path;

        private FilterField(final String path) {
            this.path = path;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument doc = new BsonDocument();
            doc.append("type", new BsonString("filter"));
            doc.append("path", new BsonString(path));
            return doc;
        }

        @Override
        public String toString() {
            return "FilterField{"
                    + "path='" + path + '\''
                    + '}';
        }
    }

    /**
     * An auto-embed field definition for a vector search index.
     *
     * <p>Instances are created via {@link #autoEmbedField(String)}.</p>
     *
     * @since 5.8
     */
    @NotThreadSafe
    public static final class AutoEmbedField implements Bson {
        private final String path;
        @Nullable
        private String modality;
        @Nullable
        private String model;
        @Nullable
        private Integer numDimensions;
        @Nullable
        private String quantization;
        @Nullable
        private String similarity;
        @Nullable
        private String indexingMethod;
        @Nullable
        private Bson hnswOptions;

        private AutoEmbedField(final String path) {
            this.path = path;
        }

        /**
         * Sets the modality for auto-embedding. This is a required field.
         *
         * <p>The initially supported type is {@code "text"}.</p>
         *
         * @param modality the modality (e.g., {@code "text"})
         * @return this
         * @since 5.8
         */
        public AutoEmbedField modality(final String modality) {
            this.modality = notNull("modality", modality);
            return this;
        }

        /**
         * Sets the embedding model to use. This is a required field.
         *
         * <p>Only one model can be used across all fields in a single vector index definition.</p>
         *
         * @param model the model name (e.g., {@code "voyage-4"}, {@code "voyage-4-large"}, {@code "voyage-4-lite"}, {@code "voyage-code-3"})
         * @return this
         * @since 5.8
         */
        public AutoEmbedField model(final String model) {
            this.model = notNull("model", model);
            return this;
        }

        /**
         * Sets the number of dimensions for the auto-embedded vector. This is an optional field.
         *
         * <p>These map to the number of dimensions supported by the API endpoint (currently 256, 512, 1024, 2048).</p>
         *
         * @param numDimensions the number of vector dimensions
         * @return this
         * @since 5.8
         */
        public AutoEmbedField numDimensions(final int numDimensions) {
            isTrueArgument("numDimensions > 0", numDimensions > 0);
            this.numDimensions = numDimensions;
            return this;
        }

        /**
         * Sets the quantization type for the auto-embedded vector. This is an optional field.
         *
         * <p>Supported values:</p>
         * <ul>
         *   <li>{@code "float"}</li>
         *   <li>{@code "scalar"}</li>
         *   <li>{@code "binary"}</li>
         *   <li>{@code "binaryNoRescore"}</li>
         * </ul>
         *
         * @param quantization the quantization type
         * @return this
         * @since 5.8
         */
        public AutoEmbedField quantization(final String quantization) {
            this.quantization = notNull("quantization", quantization);
            return this;
        }

        /**
         * Sets the similarity function used to compare vectors. This is an optional field.
         *
         * <p>Supported values:</p>
         * <ul>
         *   <li>{@code "dotProduct"}</li>
         *   <li>{@code "cosine"}</li>
         *   <li>{@code "euclidean"}</li>
         * </ul>
         *
         * @param similarity the similarity function name
         * @return this
         * @since 5.8
         */
        public AutoEmbedField similarity(final String similarity) {
            this.similarity = notNull("similarity", similarity);
            return this;
        }

        /**
         * Sets the indexing method for this auto-embed field. This is an optional field.
         *
         * <p>Supported values:</p>
         * <ul>
         *   <li>{@code "flat"} — optimized for multi-tenant use cases with singular, static filters</li>
         *   <li>{@code "hnsw"} — Hierarchical Navigable Small World graph</li>
         * </ul>
         *
         * @param indexingMethod the indexing method name
         * @return this
         * @since 5.8
         */
        public AutoEmbedField indexingMethod(final String indexingMethod) {
            this.indexingMethod = notNull("indexingMethod", indexingMethod);
            return this;
        }

        /**
         * Sets the HNSW options for this auto-embed field. This is an optional field.
         *
         * <p>This is only applicable when the indexing method is {@code "hnsw"}.
         * A convenience builder is available via {@link HnswSearchIndexOptions}, or a raw
         * {@link org.bson.Document} may be passed directly.</p>
         *
         * @param hnswOptions the HNSW options
         * @return this
         * @see HnswSearchIndexOptions
         * @since 5.8
         */
        public AutoEmbedField hnswOptions(final Bson hnswOptions) {
            this.hnswOptions = notNull("hnswOptions", hnswOptions);
            return this;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            isTrueArgument("modality is required for autoEmbed fields", modality != null);
            isTrueArgument("model is required for autoEmbed fields", model != null);
            BsonDocument doc = new BsonDocument();
            doc.append("type", new BsonString("autoEmbed"));
            doc.append("path", new BsonString(path));
            doc.append("modality", new BsonString(modality));
            doc.append("model", new BsonString(model));
            if (numDimensions != null) {
                doc.append("numDimensions", new BsonInt32(numDimensions));
            }
            if (quantization != null) {
                doc.append("quantization", new BsonString(quantization));
            }
            if (similarity != null) {
                doc.append("similarity", new BsonString(similarity));
            }
            if (indexingMethod != null) {
                doc.append("indexingMethod", new BsonString(indexingMethod));
            }
            if (hnswOptions != null) {
                doc.append("hnswOptions", hnswOptions.toBsonDocument(documentClass, codecRegistry));
            }
            return doc;
        }

        @Override
        public String toString() {
            return "AutoEmbedField{"
                    + "path='" + path + '\''
                    + ", modality='" + modality + '\''
                    + ", model='" + model + '\''
                    + ", numDimensions=" + numDimensions
                    + ", quantization='" + quantization + '\''
                    + ", similarity='" + similarity + '\''
                    + ", indexingMethod='" + indexingMethod + '\''
                    + ", hnswOptions=" + hnswOptions
                    + '}';
        }
    }
}
