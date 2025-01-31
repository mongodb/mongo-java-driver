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

package com.mongodb.client.model.mql;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.search.SearchOperator;
import com.mongodb.internal.connection.ServerHelper;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mongodb.ClusterFixture.isAtlasSearchTest;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getPrimary;
import static com.mongodb.client.model.Aggregates.search;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.search.SearchOptions.searchOptions;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AggregatesSearchTest {
    public static final String ATLAS_SEARCH_DATABASE = "javaVectorSearchTest";
    private static MongoClient client;
    private static MongoDatabase database;
    private static MongoCollection<Document> collection;
    private static String searchIndexName;

    @BeforeAll
    public static void beforeAll() {
        assumeTrue(isAtlasSearchTest());
        assumeTrue(serverVersionAtLeast(8, 0));

        client = getMongoClient();
        database = client.getDatabase(ATLAS_SEARCH_DATABASE);
        String collectionName = AggregatesSearchTest.class.getName();
        collection = database.getCollection(collectionName);
        collection.drop();

        // We insert documents first. The ensuing indexing guarantees that all
        // data present at the time indexing commences will be indexed before
        // the index enters the READY state.
        insertDocuments("[\n"
                        + "   { _id: 1 },\n"
                        + "   { _id: 2, title: null },\n"
                        + "   { _id: 3, title: 'test' },\n"
                        + "   { _id: 4, title: ['test', 'xyz'] },\n"
                        + "   { _id: 5, title: 'not test' },\n"
                        + "   { _id: 6, description: 'desc 1' },\n"
                        + "   { _id: 7, description: 'desc 8' },\n"
                        + "   { _id: 8, summary: 'summary 1 one five' },\n"
                        + "   { _id: 9, summary: 'summary 2 one two three four five' },\n"
                        + "]");

        searchIndexName = "not_default";
        // Index creation can take disproportionately long, so we create it once
        // for all tests.
        // We set dynamic to true to index unspecified fields. Different kinds
        // of fields are needed for different tests.
        collection.createSearchIndexes(Arrays.asList(new SearchIndexModel(searchIndexName, Document.parse(
                "{\n"
                + "  \"mappings\": {\n"
                + "    \"dynamic\": true,\n"
                + "    \"fields\": {\n"
                + "      \"title\": {\n"
                + "        \"type\": \"token\"\n"
                + "      },\n"
                + "      \"description\": {\n"
                + "        \"analyzer\": \"lucene.keyword\","
                + "        \"type\": \"string\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"))));
        waitForIndex(collection, searchIndexName);
    }

    @AfterAll
    public static void afterAll() {
        if (collection != null) {
            collection.drop();
        }
        try {
            ServerHelper.checkPool(getPrimary());
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Test
    public void testExists() {
        List<Bson> pipeline = Arrays.asList(
                search(SearchOperator.exists(fieldPath("title")),
                        searchOptions().index(searchIndexName)));
        assertResults(pipeline, "[\n"
                                + "   { _id: 2, title: null },\n"
                                + "   { _id: 3, title: 'test' },\n"
                                + "   { _id: 4, title: ['test', 'xyz'] },\n"
                                + "   { _id: 5, title: 'not test' },\n"
                                + "]");
    }

    @Test
    public void testEquals() {
        List<Bson> pipeline1 = Arrays.asList(
                search(SearchOperator.equals(fieldPath("title"), "test"),
                        searchOptions().index(searchIndexName)));
        assertResults(pipeline1, "[\n"
                                 + "   { _id: 3, title: 'test' }\n"
                                 + "   { _id: 4, title: ['test', 'xyz'] }\n"
                                 + "]");

        // equals null does not match non-existent fields
        List<Bson> pipeline2 = Arrays.asList(
                search(SearchOperator.equalsNull(fieldPath("title")),
                        searchOptions().index(searchIndexName)));
        assertResults(pipeline2, "[\n"
                                 + "   { _id: 2, title: null }\n"
                                 + "]");
    }

    @Test
    public void testMoreLikeThis() {
        List<Bson> pipeline = Arrays.asList(
                search(SearchOperator.moreLikeThis(Document.parse("{ summary: 'summary' }").toBsonDocument()),
                        searchOptions().index(searchIndexName)));
        assertResults(pipeline, "[\n"
                                + "   { _id: 8, summary: 'summary 1 one five' },\n"
                                + "   { _id: 9, summary: 'summary 2 one two three four five' },\n"
                                + "]");
    }

    @Test
    public void testRegex() {
        List<Bson> pipeline = Arrays.asList(
                search(SearchOperator.regex(fieldPath("description"), "des[c]+ <1-4>"),
                        searchOptions().index(searchIndexName)));
        assertResults(pipeline, "[\n"
                                + "   { _id: 6, description: 'desc 1' },\n"
                                + "]");
    }

    @Test
    public void testWildcard() {
        List<Bson> pipeline = Arrays.asList(
                search(SearchOperator.wildcard(fieldPath("description"), "desc*"),
                        searchOptions().index(searchIndexName)));
        assertResults(pipeline, "[\n"
                                + "   { _id: 6, description: 'desc 1' },\n"
                                + "   { _id: 7, description: 'desc 8' },\n"
                                + "]");
    }

    @Test
    public void testPhrase() {
        List<Bson> pipeline = Arrays.asList(
                search(SearchOperator.phrase(fieldPath("summary"), "one five").slop(2),
                        searchOptions().index(searchIndexName)));
        assertResults(pipeline, "[\n"
                                + "   { _id: 8, summary: 'summary 1 one five' },\n"
                                + "]");
    }

    @Test
    public void testQueryString() {
        List<Bson> pipeline = Arrays.asList(
                search(SearchOperator.queryString(fieldPath("summary"), "summary: one AND summary: three"),
                        searchOptions().index(searchIndexName)));
        assertResults(pipeline, "[\n"
                                + "   { _id: 9, summary: 'summary 2 one two three four five' },\n"
                                + "]");
    }

    private static void insertDocuments(final String s) {
        List<Document> documents = BsonArray.parse(s).stream()
                .map(v -> new Document(v.asDocument()))
                .collect(Collectors.toList());
        collection.insertMany(documents);
    }

    private static void assertResults(final List<Bson> pipeline, final String expectedResultsAsString) {
        ArrayList<Bson> pipeline2 = new ArrayList<>(pipeline);
        pipeline2.add(sort(ascending("_id")));

        List<BsonDocument> expectedResults = parseToList(expectedResultsAsString);
        List<BsonDocument> actualResults = aggregate(pipeline2);
        assertEquals(expectedResults, actualResults);
    }

    private static List<BsonDocument> aggregate(final List<Bson> stages) {
        AggregateIterable<Document> result = collection.aggregate(stages);
        List<BsonDocument> results = new ArrayList<>();
        result.forEach(r -> results.add(r.toBsonDocument()));
        return results;
    }

    public static List<BsonDocument> parseToList(final String s) {
        return BsonArray.parse(s).stream().map(v -> toBsonDocument(v.asDocument())).collect(Collectors.toList());
    }

    public static BsonDocument toBsonDocument(final BsonDocument bsonDocument) {
        return getDefaultCodecRegistry().get(BsonDocument.class).decode(bsonDocument.asBsonReader(), DecoderContext.builder().build());
    }

    @Nullable
    private static Document indexRecord(
            final MongoCollection<Document> collection, final String indexName) {
        return StreamSupport.stream(collection.listSearchIndexes().spliterator(), false)
                .filter(index -> indexName.equals(index.getString("name")))
                .findAny().orElse(null);
    }

    static void waitForIndex(final MongoCollection<Document> collection, final String indexName) {
        long startTime = System.nanoTime();
        long timeoutNanos = TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() - startTime < timeoutNanos) {
            Document indexRecord = indexRecord(collection, indexName);
            if (indexRecord != null) {
                if ("FAILED".equals(indexRecord.getString("status"))) {
                    throw new RuntimeException("Search index has failed status.");
                }
                if (indexRecord.getBoolean("queryable")) {
                    return;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

}
