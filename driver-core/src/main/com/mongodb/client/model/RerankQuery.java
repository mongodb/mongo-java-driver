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
import org.bson.BsonString;
import org.bson.annotations.Beta;
import org.bson.annotations.Reason;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Represents a query for the {@code $rerank} aggregation pipeline stage.
 * <p>
 * The {@code $rerank} stage is available only in MongoDB Atlas.
 * <p>
 * Use {@link #rerankQuery(String)} for a simple text query, or
 * {@link #rerankQuery(Bson)} to specify the full query document directly
 * (e.g., for future modalities like imageURL or videoURL).
 *
 * @mongodb.server.release 8.3
 * @since 5.8
 */
@Beta(Reason.SERVER)
public final class RerankQuery implements Bson {
    private final Bson query;

    private RerankQuery(final Bson query) {
        this.query = query;
    }

    /**
     * Creates a rerank query with the specified text.
     * <p>
     * This is a convenience for {@code rerankQuery(new Document("text", text))}.
     *
     * @param text the query text to rerank against.
     * @return a new {@link RerankQuery}
     */
    public static RerankQuery rerankQuery(final String text) {
        notNull("text", text);
        return new RerankQuery(new BsonDocument("text", new BsonString(text)));
    }

    /**
     * Creates a rerank query from a full query document.
     * <p>
     * Use this overload for future query modalities (e.g., imageURL, videoURL)
     * or to pass additional fields alongside text.
     *
     * @param query the query document.
     * @return a new {@link RerankQuery}
     */
    public static RerankQuery rerankQuery(final Bson query) {
        notNull("query", query);
        return new RerankQuery(query);
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        return query.toBsonDocument(documentClass, codecRegistry);
    }

    @Override
    public String toString() {
        return "RerankQuery{" + query + '}';
    }
}
