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
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.VectorSearchIndexFields.autoEmbedField;
import static com.mongodb.client.model.VectorSearchIndexFields.filterField;
import static com.mongodb.client.model.VectorSearchIndexFields.vectorField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class VectorSearchIndexFieldsTest {

    @Test
    void vectorFieldMinimal() {
        assertEquals(
                new BsonDocument("type", new BsonString("vector"))
                        .append("path", new BsonString("vec")),
                vectorField("vec").toBsonDocument()
        );
    }

    @Test
    void vectorFieldAllOptions() {
        assertEquals(
                new BsonDocument("type", new BsonString("vector"))
                        .append("path", new BsonString("embedding"))
                        .append("numDimensions", new BsonInt32(1536))
                        .append("similarity", new BsonString("cosine"))
                        .append("indexingMethod", new BsonString("hnsw"))
                        .append("hnswOptions", new BsonDocument("maxEdges", new BsonInt32(16))),
                vectorField("embedding")
                        .numDimensions(1536)
                        .similarity("cosine")
                        .indexingMethod("hnsw")
                        .hnswOptions(new HnswSearchIndexOptions().maxEdges(16))
                        .toBsonDocument()
        );
    }

    @Test
    void vectorFieldWithRawBsonHnswOptions() {
        assertEquals(
                new BsonDocument("type", new BsonString("vector"))
                        .append("path", new BsonString("vec"))
                        .append("indexingMethod", new BsonString("hnsw"))
                        .append("hnswOptions", new BsonDocument("maxEdges", new BsonInt32(32))),
                vectorField("vec")
                        .indexingMethod("hnsw")
                        .hnswOptions(new Document("maxEdges", 32))
                        .toBsonDocument()
        );
    }

    @Test
    void vectorFieldNumDimensionsRejectsZero() {
        assertThrows(IllegalArgumentException.class, () -> vectorField("vec").numDimensions(0));
    }

    @Test
    void vectorFieldNumDimensionsRejectsNegative() {
        assertThrows(IllegalArgumentException.class, () -> vectorField("vec").numDimensions(-1));
    }

    @Test
    void vectorFieldRejectsNullPath() {
        assertThrows(IllegalArgumentException.class, () -> vectorField(null));
    }

    @Test
    void vectorFieldRejectsNullSimilarity() {
        assertThrows(IllegalArgumentException.class, () -> vectorField("vec").similarity(null));
    }

    @Test
    void vectorFieldRejectsNullIndexingMethod() {
        assertThrows(IllegalArgumentException.class, () -> vectorField("vec").indexingMethod(null));
    }

    @Test
    void vectorFieldRejectsNullHnswOptions() {
        assertThrows(IllegalArgumentException.class, () -> vectorField("vec").hnswOptions(null));
    }

    @Test
    void filterFieldProducesCorrectBson() {
        assertEquals(
                new BsonDocument("type", new BsonString("filter"))
                        .append("path", new BsonString("status")),
                filterField("status").toBsonDocument()
        );
    }

    @Test
    void filterFieldRejectsNullPath() {
        assertThrows(IllegalArgumentException.class, () -> filterField(null));
    }

    @Test
    void autoEmbedFieldMinimal() {
        assertEquals(
                new BsonDocument("type", new BsonString("autoEmbed"))
                        .append("path", new BsonString("content"))
                        .append("modality", new BsonString("text"))
                        .append("model", new BsonString("voyage-4")),
                autoEmbedField("content").modality("text").model("voyage-4").toBsonDocument()
        );
    }

    @Test
    void autoEmbedFieldRejectsMissingModality() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("content").model("voyage-4").toBsonDocument());
    }

    @Test
    void autoEmbedFieldRejectsMissingModel() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("content").modality("text").toBsonDocument());
    }

    @Test
    void autoEmbedFieldAllOptions() {
        assertEquals(
                new BsonDocument("type", new BsonString("autoEmbed"))
                        .append("path", new BsonString("product.description"))
                        .append("modality", new BsonString("text"))
                        .append("model", new BsonString("voyage-4-large"))
                        .append("numDimensions", new BsonInt32(256))
                        .append("quantization", new BsonString("binary"))
                        .append("similarity", new BsonString("euclidean"))
                        .append("indexingMethod", new BsonString("hnsw"))
                        .append("hnswOptions", new BsonDocument("maxEdges", new BsonInt32(16))),
                autoEmbedField("product.description")
                        .modality("text")
                        .model("voyage-4-large")
                        .numDimensions(256)
                        .quantization("binary")
                        .similarity("euclidean")
                        .indexingMethod("hnsw")
                        .hnswOptions(new HnswSearchIndexOptions().maxEdges(16))
                        .toBsonDocument()
        );
    }

    @Test
    void autoEmbedFieldNumDimensionsRejectsZero() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("text").numDimensions(0));
    }

    @Test
    void autoEmbedFieldNumDimensionsRejectsNegative() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("text").numDimensions(-1));
    }

    @Test
    void autoEmbedFieldRejectsNullPath() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField(null));
    }

    @Test
    void autoEmbedFieldRejectsNullModality() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("text").modality(null));
    }

    @Test
    void autoEmbedFieldRejectsNullModel() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("text").model(null));
    }

    @Test
    void autoEmbedFieldRejectsNullQuantization() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("text").quantization(null));
    }

    @Test
    void autoEmbedFieldRejectsNullSimilarity() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("text").similarity(null));
    }

    @Test
    void autoEmbedFieldRejectsNullIndexingMethod() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("text").indexingMethod(null));
    }

    @Test
    void autoEmbedFieldRejectsNullHnswOptions() {
        assertThrows(IllegalArgumentException.class, () -> autoEmbedField("text").hnswOptions(null));
    }
}
