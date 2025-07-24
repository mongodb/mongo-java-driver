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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.Operations;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static com.mongodb.MongoNamespace.checkDatabaseNameValidity;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final UuidRepresentation uuidRepresentation;
    @Nullable
    private final AutoEncryptionSettings autoEncryptionSettings;

    private final TimeoutSettings timeoutSettings;
    private final OperationExecutor executor;
    private final Operations<BsonDocument> operations;

    public MongoDatabaseImpl(final String name, final CodecRegistry codecRegistry, final ReadPreference readPreference,
            final WriteConcern writeConcern, final boolean retryWrites, final boolean retryReads,
            final ReadConcern readConcern, final UuidRepresentation uuidRepresentation,
            @Nullable final AutoEncryptionSettings autoEncryptionSettings, final TimeoutSettings timeoutSettings,
            final OperationExecutor executor) {
        checkDatabaseNameValidity(name);
        this.name = notNull("name", name);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.retryReads = retryReads;
        this.readConcern = notNull("readConcern", readConcern);
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.autoEncryptionSettings = autoEncryptionSettings;
        this.timeoutSettings = timeoutSettings;
        this.executor = notNull("executor", executor);
        this.operations = new Operations<>(new MongoNamespace(name, COMMAND_COLLECTION_NAME), BsonDocument.class, readPreference,
                codecRegistry, readConcern, writeConcern, retryWrites, retryReads, timeoutSettings);
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
    @Nullable
    public Long getTimeout(final TimeUnit timeUnit) {
        Long timeoutMS = timeoutSettings.getTimeoutMS();
        return timeoutMS == null ? null : notNull("timeUnit", timeUnit).convert(timeoutMS, MILLISECONDS);
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(name, withUuidRepresentation(codecRegistry, uuidRepresentation), readPreference, writeConcern, retryWrites,
                retryReads, readConcern, uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern,
                uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern,
                uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoDatabase withReadConcern(final ReadConcern readConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern,
                uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoDatabase withTimeout(final long timeout, final TimeUnit timeUnit) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern,
                uuidRepresentation, autoEncryptionSettings, timeoutSettings.withTimeout(timeout, timeUnit), executor);
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> documentClass) {
        return new MongoCollectionImpl<>(new MongoNamespace(name, collectionName), documentClass, codecRegistry, readPreference,
                writeConcern, retryWrites, retryReads, readConcern, uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
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
        return getExecutor().execute(operations.commandRead(command, resultClass), readPreference, readConcern, clientSession);
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
        getExecutor().execute(operations.dropDatabase(), readConcern, clientSession);
    }

    @Override
    public ListCollectionNamesIterable listCollectionNames() {
        return createListCollectionNamesIterable(null);
    }

    @Override
    public ListCollectionNamesIterable listCollectionNames(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListCollectionNamesIterable(clientSession);
    }

    private ListCollectionNamesIterable createListCollectionNamesIterable(@Nullable final ClientSession clientSession) {
        return new ListCollectionNamesIterableImpl(createListCollectionsIterable(clientSession, BsonDocument.class, true));
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

    private <TResult> ListCollectionsIterableImpl<TResult> createListCollectionsIterable(@Nullable final ClientSession clientSession,
                                                                                     final Class<TResult> resultClass,
                                                                                     final boolean collectionNamesOnly) {
        return new ListCollectionsIterableImpl<>(clientSession, name, collectionNamesOnly, resultClass, codecRegistry,
                ReadPreference.primary(), executor, retryReads, timeoutSettings);
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

    private void executeCreateCollection(@Nullable final ClientSession clientSession, final String collectionName,
                                         final CreateCollectionOptions createCollectionOptions) {
        getExecutor().execute(operations.createCollection(collectionName, createCollectionOptions, autoEncryptionSettings),
                        readConcern, clientSession);
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
        return watch(Collections.emptyList());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.emptyList(), resultClass);
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
        return watch(clientSession, Collections.emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
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
        return new AggregateIterableImpl<>(clientSession, name, Document.class, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.DATABASE, retryReads, timeoutSettings);
    }

    private <TResult> ChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final ClientSession clientSession,
                                                                               final List<? extends Bson> pipeline,
                                                                               final Class<TResult> resultClass) {
        return new ChangeStreamIterableImpl<>(clientSession, name, codecRegistry, readPreference, readConcern, executor,
                pipeline, resultClass, ChangeStreamLevel.DATABASE, retryReads, timeoutSettings);
    }

    private void executeCreateView(@Nullable final ClientSession clientSession, final String viewName, final String viewOn,
                                   final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions) {
        notNull("createViewOptions", createViewOptions);
        getExecutor().execute(operations.createView(viewName, viewOn, pipeline, createViewOptions), readConcern, clientSession);
    }

    private OperationExecutor getExecutor() {
        return executor.withTimeoutSettings(timeoutSettings);
    }
}
