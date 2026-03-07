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
package com.mongodb.client.model.search;

import com.mongodb.internal.client.model.search.AbstractVectorSearchQuery;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Package-private implementation of {@link TextVectorSearchQuery}.
 */
final class TextVectorSearchQueryImpl extends AbstractVectorSearchQuery implements TextVectorSearchQuery {
    private final String text;
    @Nullable
    private final String model;

    TextVectorSearchQueryImpl(final String text, @Nullable final String model) {
        this.text = notNull("text", text);
        this.model = model;
    }

    @Override
    public TextVectorSearchQuery model(final String modelName) {
        return new TextVectorSearchQueryImpl(text, notNull("modelName", modelName));
    }

    @Override
    @Nullable
    public String getModel() {
        return model;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        return new Document("text", text).toBsonDocument(documentClass, codecRegistry);
    }

    @Override
    public String toString() {
        return "TextVectorSearchQuery{"
                + "text='" + text + '\''
                + ", model=" + (model != null ? "'" + model + '\'' : "null")
                + '}';
    }
}
