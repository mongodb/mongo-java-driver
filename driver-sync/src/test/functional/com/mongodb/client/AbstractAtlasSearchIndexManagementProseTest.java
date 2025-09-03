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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.SearchIndexType;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * See <a href="https://github.com/mongodb/specifications/blob/master/source/index-management/tests/README.md#search-index-management-helpers">Search Index Management Tests</a>
 */
public abstract class AbstractAtlasSearchIndexManagementProseTest {
    /**
     * The maximum number of attempts for waiting for changes or completion.
     * If this many attempts are made without success, the test will be marked as failed.
     */
    private static final int MAX_WAIT_ATTEMPTS = 70;

    /**
     * The duration in seconds to wait between each attempt when waiting for changes or completion.
     */
    private static final int WAIT_INTERVAL_SECONDS = 5;

    private static final String TEST_SEARCH_INDEX_NAME_1 = "test-search-index";
    private static final String TEST_SEARCH_INDEX_NAME_2 = "test-search-index-2";
    private static final Document NOT_DYNAMIC_MAPPING_DEFINITION = Document.parse(
                      "{"
                    + "  mappings: { dynamic: false }"
                    + "}");
    private static final Document DYNAMIC_MAPPING_DEFINITION = Document.parse(
                      "{"
                    + "  mappings: { dynamic: true }"
                    + "}");
    private static final Document VECTOR_SEARCH_DEFINITION = Document.parse(
                      "{"
                    + "  fields: ["
                    + "     {"
                    + "       type: 'vector',"
                    + "       path: 'plot_embedding',"
                    + "       numDimensions: 1536,"
                    + "       similarity: 'euclidean',"
                    + "     },"
                    + "  ]"
                    + "}");

    private MongoClient client = createMongoClient(getMongoClientSettings());
    private MongoDatabase db;
    private MongoCollection<Document> collection;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    protected AbstractAtlasSearchIndexManagementProseTest() {
       Assumptions.assumeTrue(serverVersionAtLeast(6, 0));
       Assumptions.assumeTrue(hasAtlasSearchIndexHelperEnabled(), "Atlas Search Index tests are disabled");
    }

    private static boolean hasAtlasSearchIndexHelperEnabled() {
        return Boolean.parseBoolean(System.getProperty("org.mongodb.test.atlas.search.index.helpers"));
    }

    @BeforeEach
    public void setUp() {
        MongoClientSettings mongoClientSettings = getMongoClientSettingsBuilder()
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.MAJORITY)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(final CommandStartedEvent event) {
                   /* This test case examines scenarios where the write or read concern is not forwarded to the server
                    for any Atlas Index Search commands. If a write or read concern is included in the command,
                    the server will return an error. */
                        if (isSearchIndexCommand(event)) {
                            BsonDocument command = event.getCommand();
                            assertFalse(command.containsKey("writeConcern"));
                            assertFalse(command.containsKey("readConcern"));
                        }
                    }

                    private boolean isSearchIndexCommand(final CommandStartedEvent event) {
                       return event.getCommand().toJson().contains("SearchIndex");
                    }
                })
                .build();

        client = createMongoClient(mongoClientSettings);
        db = client.getDatabase("test");

        String collectionName = UUID.randomUUID().toString();
        db.createCollection(collectionName);
        collection = db.getCollection(collectionName);
    }

    @AfterEach
    void cleanUp() {
        try {
            collection.drop();
            db.drop();
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("Case 1: Driver can successfully create and list search indexes")
    public void shouldCreateAndListSearchIndexes() throws InterruptedException {
        //given
        SearchIndexModel searchIndexModel = new SearchIndexModel(TEST_SEARCH_INDEX_NAME_1, NOT_DYNAMIC_MAPPING_DEFINITION);

        //when
        String createdSearchIndexName = collection.createSearchIndex(TEST_SEARCH_INDEX_NAME_1, NOT_DYNAMIC_MAPPING_DEFINITION);

        //then
        Assertions.assertEquals(TEST_SEARCH_INDEX_NAME_1, createdSearchIndexName);
        assertIndexesChanges(isQueryable(), searchIndexModel);
    }

    @Test
    @DisplayName("Case 2: Driver can successfully create multiple indexes in batch")
    public void shouldCreateMultipleIndexesInBatch() throws InterruptedException {
        //given
        SearchIndexModel searchIndexModel1 = new SearchIndexModel(TEST_SEARCH_INDEX_NAME_1, NOT_DYNAMIC_MAPPING_DEFINITION);
        SearchIndexModel searchIndexModel2 = new SearchIndexModel(TEST_SEARCH_INDEX_NAME_2, NOT_DYNAMIC_MAPPING_DEFINITION);

        //when
        List<String> searchIndexes = collection.createSearchIndexes(asList(searchIndexModel1, searchIndexModel2));

        //then
        assertThat(searchIndexes, contains(TEST_SEARCH_INDEX_NAME_1, TEST_SEARCH_INDEX_NAME_2));
        assertIndexesChanges(isQueryable(), searchIndexModel1, searchIndexModel2);
    }

    @Test
    @DisplayName("Case 3: Driver can successfully drop search indexes")
    public void shouldDropSearchIndex() throws InterruptedException {
        //given
        String createdSearchIndexName = collection.createSearchIndex(TEST_SEARCH_INDEX_NAME_1, NOT_DYNAMIC_MAPPING_DEFINITION);
        Assertions.assertEquals(TEST_SEARCH_INDEX_NAME_1, createdSearchIndexName);
        awaitIndexChanges(isQueryable(), new SearchIndexModel(TEST_SEARCH_INDEX_NAME_1, NOT_DYNAMIC_MAPPING_DEFINITION));

        //when
        collection.dropSearchIndex(TEST_SEARCH_INDEX_NAME_1);

        //then
        assertIndexDeleted();
    }

    @Test
    @DisplayName("Case 4: Driver can update a search index")
    public void shouldUpdateSearchIndex() throws InterruptedException {
        //given
        String createdSearchIndexName = collection.createSearchIndex(TEST_SEARCH_INDEX_NAME_1, NOT_DYNAMIC_MAPPING_DEFINITION);
        Assertions.assertEquals(TEST_SEARCH_INDEX_NAME_1, createdSearchIndexName);
        awaitIndexChanges(isQueryable(), new SearchIndexModel(TEST_SEARCH_INDEX_NAME_1, NOT_DYNAMIC_MAPPING_DEFINITION));

        //when
        collection.updateSearchIndex(TEST_SEARCH_INDEX_NAME_1, DYNAMIC_MAPPING_DEFINITION);

        //then
        assertIndexesChanges(isReady().and(isQueryable()), new SearchIndexModel(TEST_SEARCH_INDEX_NAME_1, DYNAMIC_MAPPING_DEFINITION));
    }

    @Test
    @DisplayName("Case 5: dropSearchIndex suppresses namespace not found errors")
    public void shouldSuppressNamespaceErrorWhenDroppingIndexWithoutCollection() {
        //given
        collection.drop();

        //when
        collection.dropSearchIndex("not existent index");
    }

    @Test
    @DisplayName("Case 7 implicit: Driver can successfully handle search index types when creating indexes")
    public void shouldHandleImplicitSearchIndexTypes() throws InterruptedException {
        //given
        String indexName = "test-search-index-case7-implicit";

        //when
        String result = collection.createSearchIndex(
                indexName,
                NOT_DYNAMIC_MAPPING_DEFINITION);

        //then
        assertEquals(indexName, result);
        awaitIndexChanges(isQueryable().and(hasSearchIndexType()), new SearchIndexModel(indexName, NOT_DYNAMIC_MAPPING_DEFINITION));
    }

    @Test
    @DisplayName("Case 7 explicit 'search' type: Driver can successfully handle search index types when creating indexes")
    public void shouldHandleExplicitSearchIndexTypes() throws InterruptedException {
        //given
        String indexName = "test-search-index-case7-explicit";

        //when
        List<String> searchIndexes = collection.createSearchIndexes(singletonList(new SearchIndexModel(
                indexName,
                NOT_DYNAMIC_MAPPING_DEFINITION,
                SearchIndexType.search())));

        //then
        assertEquals(1, searchIndexes.size());
        assertEquals(indexName, searchIndexes.get(0));
        awaitIndexChanges(isQueryable().and(hasSearchIndexType()), new SearchIndexModel(indexName, NOT_DYNAMIC_MAPPING_DEFINITION));
    }

    @Test
    @DisplayName("Case 7 explicit 'vectorSearch' type: Driver can successfully handle search index types when creating indexes")
    public void shouldHandleExplicitVectorSearchIndexTypes() throws InterruptedException {
        //given
        String indexName = "test-search-index-case7-vector";

        //when
        List<String> searchIndexes = collection.createSearchIndexes(singletonList(new SearchIndexModel(
                indexName,
                VECTOR_SEARCH_DEFINITION,
                SearchIndexType.vectorSearch())));

        //then
        assertEquals(1, searchIndexes.size());
        assertEquals(indexName, searchIndexes.get(0));
        awaitIndexChanges(isQueryable().and(hasVectorSearchIndexType()), new SearchIndexModel(indexName, NOT_DYNAMIC_MAPPING_DEFINITION));
    }

    @Test
    @DisplayName("Case 8: Driver requires explicit type to create a vector search index")
    public void shouldRequireExplicitTypeToCreateVectorSearchIndex() {
        //given
        String indexName = "test-search-index-case8-error";

        //when & then
        assertThrows(MongoCommandException.class, () -> collection.createSearchIndex(
                indexName,
                VECTOR_SEARCH_DEFINITION));
    }

    private void assertIndexDeleted() throws InterruptedException {
        int attempts = MAX_WAIT_ATTEMPTS;
        while (collection.listSearchIndexes().first() != null && checkAttempt(attempts--)) {
            await();
        }
    }

    private void assertIndexesChanges(final Predicate<Document> indexStatus, final SearchIndexModel... searchIndexModels)
            throws InterruptedException {

        Map<String, Document> createdIndexes = awaitIndexChanges(indexStatus, searchIndexModels);
        Assertions.assertEquals(searchIndexModels.length, createdIndexes.size());

        for (SearchIndexModel searchIndexModel : searchIndexModels) {
            Bson mappings = searchIndexModel.getDefinition();
            String searchIndexName = searchIndexModel.getName();

            Document createdIndex = createdIndexes.get(searchIndexName);
            Assertions.assertNotNull(createdIndex);
            Assertions.assertEquals(createdIndex.get("latestDefinition"), mappings);
        }
    }


    private Map<String, Document> awaitIndexChanges(final Predicate<Document> indexStatus, final SearchIndexModel... searchIndexModels)
            throws InterruptedException {
        int attempts = MAX_WAIT_ATTEMPTS;
        while (checkAttempt(attempts--)) {
            Map<String, Document> existingIndexes = StreamSupport.stream(collection.listSearchIndexes().spliterator(), false)
                    .filter(indexStatus)
                    .collect(Collectors.toMap(document -> document.getString("name"), Function.identity()));

            if (checkNames(existingIndexes, searchIndexModels)) {
                return existingIndexes;
            }
            await();
        }
        return Assertions.fail();
    }

    private Predicate<Document> isQueryable() {
        return document -> document.getBoolean("queryable");
    }

    private Predicate<Document> isReady() {
        return document -> "READY".equals(document.getString("status"));
    }


    private Predicate<Document> hasSearchIndexType() {
        return document -> "search".equals(document.getString("type"));
    }

    private Predicate<Document> hasVectorSearchIndexType() {
        return document -> "vectorSearch".equals(document.getString("type"));
    }



    private boolean checkAttempt(final int attempt) {
        Assertions.assertFalse(attempt <= 0, "Exceeded maximum attempts waiting for Search Index changes in Atlas cluster");
        return true;
    }

    private static void await() throws InterruptedException {
        TimeUnit.SECONDS.sleep(WAIT_INTERVAL_SECONDS);
    }

    private static boolean checkNames(final Map<String, Document> existingIndexes, final SearchIndexModel... searchIndexModels) {
        for (SearchIndexModel searchIndexModel : searchIndexModels) {
            String searchIndexName = searchIndexModel.getName();
            if (!existingIndexes.containsKey(searchIndexName)) {
                return false;
            }

        }
        return true;
    }
}
