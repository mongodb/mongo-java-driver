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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.SearchIndexType;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.Aggregates.vectorSearch;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.VectorSearchOptions.approximateVectorSearchOptions;
import static com.mongodb.client.model.search.VectorSearchOptions.exactVectorSearchOptions;
import static com.mongodb.client.model.search.VectorSearchQuery.textQuery;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * Tests for auto-embedding vector search queries.
 * Index creation and validation tests are in {@link com.mongodb.client.AbstractAtlasSearchIndexManagementProseTest}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractAutoEmbeddingVectorSearchFunctionalTest {

    private static final String FIELD_SEARCH_PATH = "plot";
    private static final String INDEX_NAME = "voyage_4";
    private static final String MOVIE_NAME = "Breathe";
    private static final String COLLECTION_NAME = "test";
    private static final CodecRegistry CODEC_REGISTRY = fromRegistries(getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider
                    .builder()
                    .automatic(true).build()));

    private MongoClient mongoClient;
    private MongoCollection<Document> documentCollection;

    @BeforeAll
    void setUpOnce() throws InterruptedException {
        //TODO-JAVA-6059 remove this assumption when Atlas Vector Search with automated embedding is generally available
        Assumptions.assumeTrue(false);

        mongoClient = getMongoClient(getMongoClientSettingsBuilder()
                .codecRegistry(CODEC_REGISTRY)
                .build());
        documentCollection = mongoClient
                .getDatabase(getDatabaseName())
                .getCollection(COLLECTION_NAME);
        documentCollection.drop();

        mongoClient.getDatabase(getDatabaseName()).createCollection(COLLECTION_NAME);
        createAutoEmbeddingIndex("voyage-4-large");
        // TODO-JAVA-6063
        // community search with automated embedding doesn't support queryable field yet
        // once supported remove the sleep and uncomment waitForIndex
        TimeUnit.SECONDS.sleep(2L);
        //waitForIndex(documentCollection, INDEX_NAME);
        insertDocumentsForEmbedding();
        // TODO-JAVA-6063 wait for embeddings to be generated
        TimeUnit.SECONDS.sleep(2L);
    }

    @AfterAll
    @SuppressWarnings("try")
    void tearDownOnce() {
        try (MongoClient ignore = mongoClient) {
            if (documentCollection != null) {
                documentCollection.drop();
            }
        }
    }

    private static String getDatabaseName() {
        return ClusterFixture.getDefaultDatabaseName();
    }

    private static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return Fixture.getMongoClientSettingsBuilder();
    }

    protected abstract MongoClient getMongoClient(MongoClientSettings settings);

    @Test
    @DisplayName("should execute vector search query using query text")
    void shouldExecuteVectorSearchQuery() {
        List<Bson> pipeline = asList(
                vectorSearch(
                        fieldPath(FIELD_SEARCH_PATH),
                        textQuery("movies about love"),
                        INDEX_NAME,
                        5L,
                        approximateVectorSearchOptions(5L)
                )
        );
        List<Document> documents = documentCollection.aggregate(pipeline).into(new ArrayList<>());

        Assertions.assertFalse(documents.isEmpty(), "Expected to get some results from vector search query");
        Assertions.assertEquals(MOVIE_NAME, documents.get(0).getString("title"));
    }

    @Test
    @DisplayName("should execute vector search query with model override")
    void shouldExecuteVectorSearchWithModelOverride() {
        List<Bson> pipeline = asList(
                vectorSearch(
                        fieldPath(FIELD_SEARCH_PATH),
                        textQuery("movies about love").model("voyage-4"),
                        INDEX_NAME,
                        5L,
                        approximateVectorSearchOptions(5L)
                )
        );
        List<Document> documents = documentCollection.aggregate(pipeline).into(new ArrayList<>());

        Assertions.assertFalse(documents.isEmpty(), "Expected to get some results from vector search query");
        Assertions.assertEquals(MOVIE_NAME, documents.get(0).getString("title"));
    }

    @Test
    @DisplayName("should execute exact vector search query")
    void shouldExecuteExactVectorSearchQuery() {
        List<Bson> pipeline = asList(
                vectorSearch(
                        fieldPath(FIELD_SEARCH_PATH),
                        textQuery("movies about love"),
                        INDEX_NAME,
                        5L,
                        exactVectorSearchOptions()
                )
        );
        List<Document> documents = documentCollection.aggregate(pipeline).into(new ArrayList<>());

        Assertions.assertFalse(documents.isEmpty(), "Expected to get some results from exact vector search query");
        Assertions.assertEquals(MOVIE_NAME, documents.get(0).getString("title"));
    }

    private void createAutoEmbeddingIndex(final String modelName) {
        SearchIndexModel indexModel = new SearchIndexModel(
                INDEX_NAME,
                new Document(
                        "fields",
                        Collections.singletonList(
                                new Document("type", "autoEmbed")
                                        .append("modality", "text")
                                        .append("model", modelName)
                                        .append("path", FIELD_SEARCH_PATH)
                        )),
                SearchIndexType.vectorSearch()
        );
        List<String> result = documentCollection.createSearchIndexes(Collections.singletonList(indexModel));
        Assertions.assertFalse(result.isEmpty());
    }

    /**
     * Documents borrowed from
     * <a href="https://github.com/mongodb-labs/ai-ml-pipeline-testing/blob/1d0213eb918ff502e774f70dd9f10f843c72dcdf/.evergreen/mongodb-community-search/self_test.py#L33">here</a>
     */
    private void insertDocumentsForEmbedding() {
        documentCollection.insertMany(asList(
                new Document()
                        .append("cast", asList("Cillian Murphy", "Emily Blunt", "Matt Damon"))
                        .append("director", "Christopher Nolan")
                        .append("genres", asList("Biography", "Drama", "History"))
                        .append("imdb", new Document()
                                .append("rating", 8.3)
                                .append("votes", 680000))
                        .append("plot", "The story of American scientist J. Robert Oppenheimer and his role in the development of the atomic bomb during World War II.")
                        .append("runtime", 180)
                        .append("title", "Oppenheimer")
                        .append("year", 2023),
                new Document()
                        .append("cast", asList("Andrew Garfield", "Claire Foy", "Hugh Bonneville"))
                        .append("director", "Andy Serkis")
                        .append("genres", asList("Biography", "Drama", "Romance"))
                        .append("imdb", new Document()
                                .append("rating", 7.2)
                                .append("votes", 42000))
                        .append("plot", "The inspiring true love story of Robin and Diana Cavendish, an adventurous couple who refuse to give up in the face of a devastating disease.")
                        .append("runtime", 118)
                        .append("title", MOVIE_NAME)
                        .append("year", 2017)
        ));
    }
}
