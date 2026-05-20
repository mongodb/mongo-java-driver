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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.doesNotContainNull;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A vector search index definition, producing a document of the form {@code {"fields": [...]}}.
 *
 * <p>Instances are created via {@link SearchIndexDefinition#vectorSearch(Bson...)}.</p>
 *
 * @see SearchIndexDefinition
 * @see SearchIndexType#vectorSearch()
 * @since 5.8
 */
public final class VectorSearchIndexDefinition implements SearchIndexDefinition {
    private final List<? extends Bson> fields;

    VectorSearchIndexDefinition(final List<? extends Bson> fields) {
        doesNotContainNull("fields", notNull("fields", fields));
        this.fields = new ArrayList<>(fields);
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonArray fieldArray = new BsonArray();
        for (Bson field : fields) {
            fieldArray.add(field.toBsonDocument(documentClass, codecRegistry));
        }
        return new BsonDocument("fields", fieldArray);
    }

    @Override
    public String toString() {
        return "VectorSearchIndexDefinition{"
                + "fields=" + fields
                + '}';
    }
}
