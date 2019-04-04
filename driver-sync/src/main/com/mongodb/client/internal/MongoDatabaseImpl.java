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

package com.mongodb.client.internal;

import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.changestream.ChangeStreamLevel;
import com.mongodb.client.model.AggregationLevel;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.CreateViewOperation;
import com.mongodb.operation.DropDatabaseOperation;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.MongoNamespace.checkDatabaseNameValidity;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final OperationExecutor executor;

    public MongoDatabaseImpl(final String name, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                             final WriteConcern writeConcern, final boolean retryWrites, final boolean retryReads,
                             final ReadConcern readConcern, final OperationExecutor executor) {
        checkDatabaseNameValidity(name);
        this.name = notNull("name", name);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.retryReads = retryReads;
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern, executor);
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern, executor);
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern, executor);
    }

    @Override
    public MongoDatabase withReadConcern(final ReadConcern readConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern, executor);
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> documentClass) {
        return new MongoCollectionImpl<TDocument>(new MongoNamespace(name, collectionName), documentClass, codecRegistry, readPreference,
                writeConcern, retryWrites, retryReads, readConcern, executor);
    }

    @Override
    public Document runCommand(final Bson command) {
        return runCommand(command, Document.class);
    }

    @Override
    public Document runCommand(final Bson command, final ReadPreference readPreference) {
        return runCommand(command, readPreference, Document.class);
    }

    @Override
    public <TResult> TResult runCommand(final Bson command, final Class<TResult> resultClass) {
        return runCommand(command, ReadPreference.primary(), resultClass);
    }

    @Override
    public <TResult> TResult runCommand(final Bson command, final ReadPreference readPreference, final Class<TResult> resultClass) {
        return executeCommand(null, command, readPreference, resultClass);
    }

    @Override
    public Document runCommand(final ClientSession clientSession, final Bson command) {
        return runCommand(clientSession, command, ReadPreference.primary(), Document.class);
    }

    @Override
    public Document runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference) {
        return runCommand(clientSession, command, readPreference, Document.class);
    }

    @Override
    public <TResult> TResult runCommand(final ClientSession clientSession, final Bson command, final Class<TResult> resultClass) {
        return runCommand(clientSession, command, ReadPreference.primary(), resultClass);
    }

    @Override
    public <TResult> TResult runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference,
                                        final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return executeCommand(clientSession, command, readPreference, resultClass);
    }

    private <TResult> TResult executeCommand(@Nullable final ClientSession clientSession, final Bson command,
                                             final ReadPreference readPreference, final Class<TResult> resultClass) {
        notNull("readPreference", readPreference);
        if (clientSession != null && clientSession.hasActiveTransaction() && !readPreference.equals(ReadPreference.primary())) {
            throw new MongoClientException("Read preference in a transaction must be primary");
        }
        return executor.execute(new CommandReadOperation<TResult>(getName(), toBsonDocument(command), codecRegistry.get(resultClass)),
                readPreference, readConcern, clientSession);
    }

    @Override
    public void drop() {
        executeDrop(null);
    }

    @Override
    public void drop(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        executeDrop(clientSession);
    }

    private void executeDrop(@Nullable final ClientSession clientSession) {
        executor.execute(new DropDatabaseOperation(name, getWriteConcern()), readConcern, clientSession);
    }

    @Override
    public MongoIterable<String> listCollectionNames() {
        return createListCollectionNamesIterable(null);
    }

    @Override
    public MongoIterable<String> listCollectionNames(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListCollectionNamesIterable(clientSession);
    }

    private MongoIterable<String> createListCollectionNamesIterable(@Nullable final ClientSession clientSession) {
        return createListCollectionsIterable(clientSession, BsonDocument.class, true)
                .map(new Function<BsonDocument, String>() {
                    @Override
                    public String apply(final BsonDocument result) {
                        return result.getString("name").getValue();
                    }
                });
    }

    @Override
    public ListCollectionsIterable<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(final Class<TResult> resultClass) {
        return createListCollectionsIterable(null, resultClass, false);
    }

    @Override
    public ListCollectionsIterable<Document> listCollections(final ClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(final ClientSession clientSession, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createListCollectionsIterable(clientSession, resultClass, false);
    }

    private <TResult> ListCollectionsIterable<TResult> createListCollectionsIterable(@Nullable final ClientSession clientSession,
                                                                                     final Class<TResult> resultClass,
                                                                                     final boolean collectionNamesOnly) {
        return MongoIterables.listCollectionsOf(clientSession, name, collectionNamesOnly, resultClass, codecRegistry,
                ReadPreference.primary(), executor, retryReads);
    }

    @Override
    public void createCollection(final String collectionName) {
        createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public void createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        executeCreateCollection(null, collectionName, createCollectionOptions);
    }

    @Override
    public void createCollection(final ClientSession clientSession, final String collectionName) {
        createCollection(clientSession, collectionName, new CreateCollectionOptions());
    }

    @Override
    public void createCollection(final ClientSession clientSession, final String collectionName,
                                 final CreateCollectionOptions createCollectionOptions) {
        notNull("clientSession", clientSession);
        executeCreateCollection(clientSession, collectionName, createCollectionOptions);
    }

    @SuppressWarnings("deprecation")
    private void executeCreateCollection(@Nullable final ClientSession clientSession, final String collectionName,
                                         final CreateCollectionOptions createCollectionOptions) {
        CreateCollectionOperation operation = new CreateCollectionOperation(name, collectionName, writeConcern)
                .collation(createCollectionOptions.getCollation())
                .capped(createCollectionOptions.isCapped())
                .sizeInBytes(createCollectionOptions.getSizeInBytes())
                .autoIndex(createCollectionOptions.isAutoIndex())
                .maxDocuments(createCollectionOptions.getMaxDocuments())
                .usePowerOf2Sizes(createCollectionOptions.isUsePowerOf2Sizes())
                .storageEngineOptions(toBsonDocument(createCollectionOptions.getStorageEngineOptions()));

        IndexOptionDefaults indexOptionDefaults = createCollectionOptions.getIndexOptionDefaults();
        Bson storageEngine = indexOptionDefaults.getStorageEngine();
        if (storageEngine != null) {
            operation.indexOptionDefaults(new BsonDocument("storageEngine", toBsonDocument(storageEngine)));
        }
        ValidationOptions validationOptions = createCollectionOptions.getValidationOptions();
        Bson validator = validationOptions.getValidator();
        if (validator != null) {
            operation.validator(toBsonDocument(validator));
        }
        if (validationOptions.getValidationLevel() != null) {
            operation.validationLevel(validationOptions.getValidationLevel());
        }
        if (validationOptions.getValidationAction() != null) {
            operation.validationAction(validationOptions.getValidationAction());
        }
        executor.execute(operation, readConcern, clientSession);
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline) {
        createView(viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                           final CreateViewOptions createViewOptions) {
        executeCreateView(null, viewName, viewOn, pipeline, createViewOptions);
    }

    @Override
    public void createView(final ClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline) {
        createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public void createView(final ClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions) {
        notNull("clientSession", clientSession);
        executeCreateView(clientSession, viewName, viewOn, pipeline, createViewOptions);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return watch(Collections.<Bson>emptyList());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createChangeStreamIterable(null, pipeline, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.<Bson>emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createChangeStreamIterable(clientSession, pipeline, resultClass);
    }

    @Override
    public AggregateIterable<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createAggregateIterable(null, pipeline, resultClass);
    }

    @Override
    public AggregateIterable<Document> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createAggregateIterable(clientSession, pipeline, resultClass);
    }

    private <TResult> AggregateIterable<TResult> createAggregateIterable(@Nullable final ClientSession clientSession,
                                                                         final List<? extends Bson> pipeline,
                                                                         final Class<TResult> resultClass) {
        return MongoIterables.aggregateOf(clientSession, name, Document.class, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.DATABASE, retryReads);
    }

    private <TResult> ChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final ClientSession clientSession,
                                                                               final List<? extends Bson> pipeline,
                                                                               final Class<TResult> resultClass) {
        return MongoIterables.changeStreamOf(clientSession, name, codecRegistry, readPreference,
                readConcern, executor, pipeline, resultClass, ChangeStreamLevel.DATABASE, retryReads);
    }

    private void executeCreateView(@Nullable final ClientSession clientSession, final String viewName, final String viewOn,
                                   final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions) {
        notNull("createViewOptions", createViewOptions);
        executor.execute(new CreateViewOperation(name, viewName, viewOn, createBsonDocumentList(pipeline), writeConcern)
                        .collation(createViewOptions.getCollation()),
                readConcern, clientSession);
    }

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        notNull("pipeline", pipeline);
        List<BsonDocument> bsonDocumentPipeline = new ArrayList<BsonDocument>(pipeline.size());
        for (Bson obj : pipeline) {
            if (obj == null) {
                throw new IllegalArgumentException("pipeline can not contain a null value");
            }
            bsonDocumentPipeline.add(obj.toBsonDocument(BsonDocument.class, codecRegistry));
        }
        return bsonDocumentPipeline;
    }

    @Nullable
    private BsonDocument toBsonDocument(@Nullable final Bson document) {
        return document == null ? null : document.toBsonDocument(BsonDocument.class, codecRegistry);
    }
}
