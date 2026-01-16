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

package com.mongodb.client.vector;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.OperationTest;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.SearchIndexType;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.Aggregates.vectorSearch;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.VectorSearchOptions.approximateVectorSearchOptions;
import static com.mongodb.client.model.search.VectorSearchQuery.textQuery;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * The test cases were borrowed from
 * <a href="https://github.com/mongodb-labs/ai-ml-pipeline-testing/blob/main/.evergreen/mongodb-community-search/self_test.py"> this repository</a>.
 */
public abstract class AbstractAutomatedEmbeddingVectorSearchFunctionalTest extends OperationTest {

    private static final String FIELD_SEARCH_PATH = "plot";
    // as of 2025-01-13 only voyage 4 is supported for automated embedding
    // it might change in the future so for now we are only testing with voyage-4-large model
    private static final String INDEX_NAME = "voyage_4";

    private static final String MOVIE_NAME = "Breathe";
    private static final CodecRegistry CODEC_REGISTRY = fromRegistries(getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider
                    .builder()
                    .automatic(true).build()));
    private MongoCollection<Document> documentCollection;

    private MongoClient mongoClient;

    @BeforeEach
    public void setUp() {
        //TODO-JAVA-6059 remove this line when Atlas Vector Search with automated embedding is generally available
        // right now atlas search with automated embedding is in private preview and
        // only available via a custom docker image
        Assumptions.assumeTrue(false);

        super.beforeEach();
        mongoClient = getMongoClient(getMongoClientSettingsBuilder()
                .codecRegistry(CODEC_REGISTRY)
                .build());
        documentCollection = mongoClient
                .getDatabase(getDatabaseName())
                .getCollection(getCollectionName());
    }

    @AfterEach
    @SuppressWarnings("try")
    public void afterEach() {
        try (MongoClient ignore = mongoClient) {
            super.afterEach();
        }
    }

    private static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return Fixture.getMongoClientSettingsBuilder();
    }

    protected abstract MongoClient getMongoClient(MongoClientSettings settings);

    /**
     * Happy path for automated embedding with Voyage-4 model.
     *
     * <p>Steps:
     * <ol>
     *   <li>Create empty collection</li>
     *   <li>Create auto-embedding search index with voyage-4-large model</li>
     *   <li>Insert movie documents</li>
     *   <li>Run vector search query using query text</li>
     * </ol>
     *
     * <p>Expected: Query returns "Breathe" as the top match for "movies about love"
     */
    @Test
    @DisplayName("should create auto embedding index and run vector search query using query text")
    void shouldCreateAutoEmbeddingIndexAndRunVectorSearchQuery() throws InterruptedException {
        mongoClient.getDatabase(getDatabaseName()).createCollection(getCollectionName());
        createAutoEmbeddingIndex("voyage-4-large");
        // TODO-JAVA-6063
        // community search with automated embedding doesn't support queryable field yet
        // once supported remove the sleep and uncomment waitForIndex
        TimeUnit.SECONDS.sleep(2L);
        //waitForIndex(documentCollection, INDEX_NAME);
        insertDocumentsForEmbedding();
        // TODO-JAVA-6063 wait for embeddings to be generated
        // once there is an official way to check the index status, we should use it instead of sleep
        // there is a workaround to pass a feature flag `internalListAllIndexesForTesting` but it's not official yet
        TimeUnit.SECONDS.sleep(2L);
        runEmbeddingQuery();
    }

    @Test
    @DisplayName("should fail when invalid model name was used")
    void shouldFailWhenInvalidModelNameWasUsed() {
        mongoClient.getDatabase(getDatabaseName()).createCollection(getCollectionName());
        Assertions.assertThrows(
                MongoCommandException.class,
                () -> createAutoEmbeddingIndex("test"),
                "Valid voyage model name was not used"
        );
    }

    @Test
    @DisplayName("should fail to create auto embedding index without model")
    void shouldFailToCreateAutoEmbeddingIndexWithoutModel() {
        mongoClient.getDatabase(getDatabaseName()).createCollection(getCollectionName());
        SearchIndexModel indexModel = new SearchIndexModel(
                INDEX_NAME,
                new Document(
                        "fields",
                        Collections.singletonList(
                                new Document("type", "autoEmbed")
                                        .append("modality", "text")
                                        .append("path", FIELD_SEARCH_PATH)
                        )),
                SearchIndexType.vectorSearch()
        );
        Assertions.assertThrows(
                MongoCommandException.class,
                () -> documentCollection.createSearchIndexes(Collections.singletonList(indexModel)),
                "Expected index creation to fail because model is not specified"
        );
    }

    private void runEmbeddingQuery() {
        List<Bson> pipeline = asList(
                vectorSearch(
                        fieldPath(FIELD_SEARCH_PATH),
                        textQuery("movies about love"),
                        INDEX_NAME,
                        5L, // limit
                        approximateVectorSearchOptions(5L) // numCandidates
                )
        );
        final List<Document> documents = documentCollection.aggregate(pipeline).into(new ArrayList<>());

        Assertions.assertFalse(documents.isEmpty(), "Expected to get some results from vector search query");
        Assertions.assertEquals(MOVIE_NAME, documents.get(0).getString("title"));
    }

    /**
     * All the documents were borrowed from
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

    private void createAutoEmbeddingIndex(final String modelName) {
        SearchIndexModel indexModel = new SearchIndexModel(
                INDEX_NAME,
                new Document(
                        "fields",
                        Collections.singletonList(
                                new Document("type", "autoEmbed") // type autoEmbed accepts a text
                                        .append("modality", "text")
                                        .append("model", modelName)
                                        .append("path", FIELD_SEARCH_PATH)
                        )),
                SearchIndexType.vectorSearch()
        );
        List<String> result = documentCollection.createSearchIndexes(Collections.singletonList(indexModel));

        Assertions.assertFalse(result.isEmpty());
    }
}
