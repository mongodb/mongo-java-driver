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
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget;
import static com.mongodb.ClusterFixture.getAsyncBinding;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

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

    CollectionHelper<Document> getCollectionHelper() {
        return getCollectionHelper(getNamespace());
    }

    private CollectionHelper<Document> getCollectionHelper(final MongoNamespace namespace) {
        return new CollectionHelper<>(new DocumentCodec(), namespace);
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

    public static List<Document> parseToList(final String s) {
        return BsonArray.parse(s).stream().map(v -> toDocument(v.asDocument())).collect(Collectors.toList());
    }

    public static Document toDocument(final BsonDocument bsonDocument) {
        return getDefaultCodecRegistry().get(Document.class).decode(bsonDocument.asBsonReader(), DecoderContext.builder().build());
    }
}
