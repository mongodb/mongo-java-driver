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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListCollectionNamesPublisher;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoNamespace.checkDatabaseNameValidity;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * The internal MongoDatabase implementation.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class MongoDatabaseImpl implements MongoDatabase {
    private final MongoOperationPublisher<Document> mongoOperationPublisher;

    MongoDatabaseImpl(final MongoOperationPublisher<Document> mongoOperationPublisher) {
        this.mongoOperationPublisher = notNull("publisherHelper", mongoOperationPublisher);
        checkDatabaseNameValidity(getName());
    }

    @Override
    public String getName() {
        return assertNotNull(mongoOperationPublisher.getNamespace()).getDatabaseName();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return mongoOperationPublisher.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return mongoOperationPublisher.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return mongoOperationPublisher.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return mongoOperationPublisher.getReadConcern();
    }

    @Override
    public Long getTimeout(final TimeUnit timeUnit) {
        Long timeoutMS = mongoOperationPublisher.getTimeoutSettings().getTimeoutMS();
        return timeoutMS == null ? null : notNull("timeUnit", timeUnit).convert(timeoutMS, MILLISECONDS);
    }

    MongoOperationPublisher<Document> getMongoOperationPublisher() {
        return mongoOperationPublisher;
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(mongoOperationPublisher.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(mongoOperationPublisher.withReadPreference(readPreference));
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(mongoOperationPublisher.withWriteConcern(writeConcern));
    }

    @Override
    public MongoDatabase withReadConcern(final ReadConcern readConcern) {
        return new MongoDatabaseImpl(mongoOperationPublisher.withReadConcern(readConcern));
    }

    @Override
    public MongoDatabase withTimeout(final long timeout, final TimeUnit timeUnit) {
        return new MongoDatabaseImpl(mongoOperationPublisher.withTimeout(timeout, timeUnit));
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return new MongoCollectionImpl<>(
                mongoOperationPublisher.withNamespaceAndDocumentClass(new MongoNamespace(getName(), collectionName), clazz));
    }

    @Override
    public Publisher<Document> runCommand(final Bson command) {
        return runCommand(command, Document.class);
    }

    @Override
    public Publisher<Document> runCommand(final Bson command, final ReadPreference readPreference) {
        return runCommand(command, readPreference, Document.class);
    }

    @Override
    public <T> Publisher<T> runCommand(final Bson command, final Class<T> clazz) {
        return runCommand(command, ReadPreference.primary(), clazz);
    }

    @Override
    public <T> Publisher<T> runCommand(final Bson command, final ReadPreference readPreference, final Class<T> clazz) {
        return mongoOperationPublisher.runCommand(null, command, readPreference, clazz);
    }

    @Override
    public Publisher<Document> runCommand(final ClientSession clientSession, final Bson command) {
        return runCommand(clientSession, command, Document.class);
    }

    @Override
    public Publisher<Document> runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference) {
        return runCommand(clientSession, command, readPreference, Document.class);
    }

    @Override
    public <T> Publisher<T> runCommand(final ClientSession clientSession, final Bson command, final Class<T> clazz) {
        return runCommand(clientSession, command, ReadPreference.primary(), clazz);
    }

    @Override
    public <T> Publisher<T> runCommand(final ClientSession clientSession, final Bson command,
                                       final ReadPreference readPreference, final Class<T> clazz) {
        return mongoOperationPublisher.runCommand(notNull("clientSession", clientSession), command, readPreference, clazz);
    }

    @Override
    public Publisher<Void> drop() {
        return mongoOperationPublisher.dropDatabase(null);
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession) {
        return mongoOperationPublisher.dropDatabase(notNull("clientSession", clientSession));
    }

    @Override
    public ListCollectionNamesPublisher listCollectionNames() {
        return new ListCollectionNamesPublisherImpl(new ListCollectionsPublisherImpl<>(null, mongoOperationPublisher, true));
    }

    @Override
    public ListCollectionNamesPublisher listCollectionNames(final ClientSession clientSession) {
        return new ListCollectionNamesPublisherImpl(
                new ListCollectionsPublisherImpl<>(notNull("clientSession", clientSession), mongoOperationPublisher, true));
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <C> ListCollectionsPublisher<C> listCollections(final Class<C> clazz) {
        return new ListCollectionsPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(clazz), false);
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections(final ClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <C> ListCollectionsPublisher<C> listCollections(final ClientSession clientSession, final Class<C> clazz) {
        return new ListCollectionsPublisherImpl<>(notNull("clientSession", clientSession),
                                                  mongoOperationPublisher.withDocumentClass(clazz), false);
    }

    @Override
    public Publisher<Void> createCollection(final String collectionName) {
        return createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public Publisher<Void> createCollection(final String collectionName, final CreateCollectionOptions options) {
        return mongoOperationPublisher.createCollection(null,
                notNull("collectionName", collectionName), notNull("options", options));
    }

    @Override
    public Publisher<Void> createCollection(final ClientSession clientSession, final String collectionName) {
        return createCollection(clientSession, collectionName, new CreateCollectionOptions());
    }

    @Override
    public Publisher<Void> createCollection(final ClientSession clientSession, final String collectionName,
                                            final CreateCollectionOptions options) {
        return mongoOperationPublisher.createCollection(notNull("clientSession", clientSession),
                notNull("collectionName", collectionName), notNull("options", options));
    }

    @Override
    public Publisher<Void> createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline) {
        return createView(viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public Publisher<Void> createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                                      final CreateViewOptions options) {
        return mongoOperationPublisher.createView(null, viewName, viewOn, pipeline, options);
    }

    @Override
    public Publisher<Void> createView(final ClientSession clientSession, final String viewName, final String viewOn,
                                      final List<? extends Bson> pipeline) {
        return createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public Publisher<Void> createView(final ClientSession clientSession, final String viewName, final String viewOn,
                                      final List<? extends Bson> pipeline, final CreateViewOptions options) {
        return mongoOperationPublisher.createView(notNull("clientSession", clientSession), viewName, viewOn, pipeline, options);
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return watch(Collections.emptyList());
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final Class<T> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final List<? extends Bson> pipeline, final Class<T> resultClass) {
        return new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher, resultClass, pipeline, ChangeStreamLevel.DATABASE);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList(), Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final ClientSession clientSession, final Class<T> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                              final Class<T> resultClass) {
        return new ChangeStreamPublisherImpl<>(notNull("clientSession", clientSession), mongoOperationPublisher,
                                               resultClass, pipeline, ChangeStreamLevel.DATABASE);
    }

    @Override
    public AggregatePublisher<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <T> AggregatePublisher<T> aggregate(final List<? extends Bson> pipeline, final Class<T> resultClass) {
        return new AggregatePublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(resultClass), pipeline,
                                            AggregationLevel.DATABASE);
    }

    @Override
    public AggregatePublisher<Document> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, Document.class);
    }

    @Override
    public <T> AggregatePublisher<T> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                               final Class<T> resultClass) {
        return new AggregatePublisherImpl<>(notNull("clientSession", clientSession),
                                            mongoOperationPublisher.withDocumentClass(resultClass), pipeline, AggregationLevel.DATABASE);
    }

}
