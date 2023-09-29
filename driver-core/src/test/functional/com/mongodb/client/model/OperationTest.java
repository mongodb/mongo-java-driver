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
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.internal.connection.ServerHelper;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.TIMEOUT;
import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget;
import static com.mongodb.ClusterFixture.getAsyncBinding;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.Aggregates.setWindowFields;
import static com.mongodb.client.model.Aggregates.sort;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class OperationTest {

    protected static final DocumentCodec DOCUMENT_DECODER = new DocumentCodec();
    protected static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();

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

    protected String getDatabaseName() {
        return ClusterFixture.getDefaultDatabaseName();
    }

    protected String getCollectionName() {
        return "test";
    }

    protected MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabaseName(), getCollectionName());
    }

    static List<BsonDocument> parseToList(final String s) {
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

    protected void assertResults(final List<Bson> pipeline, final String expectedResultsAsString,
            final int scale, final RoundingMode roundingMode) {
        List<BsonDocument> expectedResults = parseToList(expectedResultsAsString);
        List<BsonDocument> results = getCollectionHelper().aggregate(pipeline);
        assertEquals(adjustScale(expectedResults, scale, roundingMode), adjustScale(results, scale, roundingMode));
    }

    private static List<BsonDocument> adjustScale(final List<BsonDocument> documents, final int scale, final RoundingMode roundingMode) {
        documents.replaceAll(value -> adjustScale(value, scale, roundingMode).asDocument());
        return documents;
    }

    private static BsonValue adjustScale(final BsonValue value, final int scale, final RoundingMode roundingMode) {
        if (value.isDouble()) {
            double scaledDoubleValue = BigDecimal.valueOf(value.asDouble().doubleValue())
                    .setScale(scale, roundingMode)
                    .doubleValue();
            return new BsonDouble(scaledDoubleValue);
        } else if (value.isDocument()) {
            for (Map.Entry<String, BsonValue> entry : value.asDocument().entrySet()) {
                entry.setValue(adjustScale(entry.getValue(), scale, roundingMode));
            }
        } else if (value.isArray()) {
            BsonArray array = value.asArray();
            for (int i = 0; i < array.size(); i++) {
                array.set(i, adjustScale(array.get(i), scale, roundingMode));
            }
        }
        return value;
    }

    protected List<Object> aggregateWithWindowFields(@Nullable final Object partitionBy,
                                                     final WindowOutputField output,
                                                     final Bson sortSpecification) {
        List<Bson> stages = new ArrayList<>();
        stages.add(setWindowFields(partitionBy, null, output));
        stages.add(sort(sortSpecification));

        List<Document> actual = getCollectionHelper().aggregate(stages, DOCUMENT_DECODER);

        return actual.stream()
                .map(doc -> doc.get("result"))
                .collect(toList());
    }

    protected <T> void ifNotNull(@Nullable final T maybeNull, final Consumer<T> consumer) {
        if (maybeNull != null) {
            consumer.accept(maybeNull);
        }
    }

    protected void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    protected <T> T block(final Consumer<FutureResultCallback<T>> consumer) {
        FutureResultCallback<T> cb = new FutureResultCallback<>();
        consumer.accept(cb);
        return cb.get(TIMEOUT, TimeUnit.SECONDS);
    }
}
