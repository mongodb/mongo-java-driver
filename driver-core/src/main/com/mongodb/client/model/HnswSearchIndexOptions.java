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
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * Options for the HNSW (Hierarchical Navigable Small World) indexing method in a vector search index.
 *
 * <p>This class provides a fluent builder for specifying HNSW-specific parameters when creating
 * a vector search index with {@code indexingMethod("hnsw")}.</p>
 *
 * <p>Since {@link VectorSearchIndexFields.VectorField#hnswOptions(Bson)} accepts any {@link Bson},
 * a raw {@link org.bson.Document} may also be passed directly for forward compatibility.</p>
 *
 * <pre>{@code
 *    vectorField("embedding")
 *        .indexingMethod("hnsw")
 *        .hnswOptions(new HnswSearchIndexOptions().maxEdges(16).numEdgeCandidates(200))
 * }</pre>
 *
 * @see VectorSearchIndexFields.VectorField#hnswOptions(Bson)
 * @since 5.8
 */
@NotThreadSafe
public final class HnswSearchIndexOptions implements Bson {
    @Nullable
    private Integer maxEdges;
    @Nullable
    private Integer numEdgeCandidates;

    /**
     * Creates a new instance with default settings.
     *
     * @since 5.8
     */
    public HnswSearchIndexOptions() {
    }

    /**
     * Sets the maximum number of connected neighbors for each node in the HNSW graph.
     *
     * @param maxEdges the maximum number of edges (connected neighbors)
     * @return this
     * @since 5.8
     */
    public HnswSearchIndexOptions maxEdges(final int maxEdges) {
        isTrueArgument("maxEdges > 0", maxEdges > 0);
        this.maxEdges = maxEdges;
        return this;
    }

    /**
     * Sets the number of nearest neighbor candidates to consider when building the HNSW graph.
     *
     * @param numEdgeCandidates the number of nearest neighbor candidates
     * @return this
     * @since 5.8
     */
    public HnswSearchIndexOptions numEdgeCandidates(final int numEdgeCandidates) {
        isTrueArgument("numEdgeCandidates > 0", numEdgeCandidates > 0);
        this.numEdgeCandidates = numEdgeCandidates;
        return this;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument doc = new BsonDocument();
        if (maxEdges != null) {
            doc.append("maxEdges", new BsonInt32(maxEdges));
        }
        if (numEdgeCandidates != null) {
            doc.append("numEdgeCandidates", new BsonInt32(numEdgeCandidates));
        }
        return doc;
    }

    @Override
    public String toString() {
        return "HnswSearchIndexOptions{"
                + "maxEdges=" + maxEdges
                + ", numEdgeCandidates=" + numEdgeCandidates
                + '}';
    }
}
