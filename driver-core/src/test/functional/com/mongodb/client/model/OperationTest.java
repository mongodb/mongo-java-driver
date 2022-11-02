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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoNamespace;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.internal.connection.ServerHelper;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget;
import static com.mongodb.ClusterFixture.getAsyncBinding;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class OperationTest {

    @BeforeEach
    public void beforeEach() {
        ServerHelper.checkPool(getPrimary());
        CollectionHelper.drop(getNamespace());
    }

    @AfterEach
    public void afterEach() {
        CollectionHelper.drop(getNamespace());
        checkReferenceCountReachesTarget(getBinding(), 1);
        checkReferenceCountReachesTarget(getAsyncBinding(), 1);
        ServerHelper.checkPool(getPrimary());
    }

    protected CollectionHelper<BsonDocument> getCollectionHelper() {
        return getCollectionHelper(getNamespace());
    }

    private CollectionHelper<BsonDocument> getCollectionHelper(final MongoNamespace namespace) {
        return new CollectionHelper<>(new BsonDocumentCodec(), namespace);
    }

    private String getDatabaseName() {
        return ClusterFixture.getDefaultDatabaseName();
    }

    private String getCollectionName() {
        return "test";
    }

    MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabaseName(), getCollectionName());
    }

    private static List<BsonDocument> parseToList(final String s) {
        return BsonArray.parse(s).stream().map(v -> toBsonDocument(v.asDocument())).collect(Collectors.toList());
    }

    public static BsonDocument toBsonDocument(final BsonDocument bsonDocument) {
        return getDefaultCodecRegistry().get(BsonDocument.class).decode(bsonDocument.asBsonReader(), DecoderContext.builder().build());
    }


    protected List<Bson> assertPipeline(final String stageAsString, final Bson stage) {
        List<Bson> pipeline = Collections.singletonList(stage);
        return assertPipeline(stageAsString, pipeline);
    }

    protected List<Bson> assertPipeline(final String stageAsString, final List<Bson> pipeline) {
        BsonDocument expectedStage = BsonDocument.parse(stageAsString);
        assertEquals(expectedStage, pipeline.get(0).toBsonDocument(BsonDocument.class, getDefaultCodecRegistry()));
        return pipeline;
    }

    protected void assertResults(final List<Bson> pipeline, final String expectedResultsAsString) {
        List<BsonDocument> expectedResults = parseToList(expectedResultsAsString);
        List<BsonDocument> results = getCollectionHelper().aggregate(pipeline);
        assertEquals(expectedResults, results);
    }
}
