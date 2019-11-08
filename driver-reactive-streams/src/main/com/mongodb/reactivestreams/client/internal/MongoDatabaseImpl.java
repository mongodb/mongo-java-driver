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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.internal.async.client.AsyncMongoDatabase;
import com.mongodb.internal.async.client.Observables;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;


/**
 * The internal MongoDatabase implementation.
 *
 * <p>This should not be considered a part of the public API.</p>
 */
public final class MongoDatabaseImpl implements MongoDatabase {

    private final AsyncMongoDatabase wrapped;

    MongoDatabaseImpl(final AsyncMongoDatabase wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return wrapped.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(wrapped.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(wrapped.withReadPreference(readPreference));
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(wrapped.withWriteConcern(writeConcern));
    }

    @Override
    public MongoDatabase withReadConcern(final ReadConcern readConcern) {
        return new MongoDatabaseImpl(wrapped.withReadConcern(readConcern));
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> clazz) {
        return new MongoCollectionImpl<>(wrapped.getCollection(collectionName, clazz));
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
    public <TResult> Publisher<TResult> runCommand(final Bson command, final Class<TResult> clazz) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.runCommand(command, clazz, callback));
    }

    @Override
    public <TResult> Publisher<TResult> runCommand(final Bson command, final ReadPreference readPreference,
                                                   final Class<TResult> clazz) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.runCommand(command, readPreference, clazz, callback));
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
    public <TResult> Publisher<TResult> runCommand(final ClientSession clientSession, final Bson command, final Class<TResult> clazz) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.runCommand(clientSession.getWrapped(), command, clazz, callback));
    }

    @Override
    public <TResult> Publisher<TResult> runCommand(final ClientSession clientSession, final Bson command,
                                                   final ReadPreference readPreference, final Class<TResult> clazz) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.runCommand(clientSession.getWrapped(), command, readPreference, clazz, callback));
    }

    @Override
    public Publisher<Void> drop() {
        return new SingleResultObservableToPublisher<>(wrapped::drop);
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession) {
        return new SingleResultObservableToPublisher<>(callback -> wrapped.drop(clientSession.getWrapped(), callback));
    }

    @Override
    public Publisher<String> listCollectionNames() {
        return new ObservableToPublisher<>(Observables.observe(wrapped.listCollectionNames()));
    }

    @Override
    public Publisher<String> listCollectionNames(final ClientSession clientSession) {
        return new ObservableToPublisher<>(Observables.observe(
                wrapped.listCollectionNames(clientSession.getWrapped()))
        );
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <C> ListCollectionsPublisher<C> listCollections(final Class<C> clazz) {
        return new ListCollectionsPublisherImpl<C>(wrapped.listCollections(clazz));
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections(final ClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <C> ListCollectionsPublisher<C> listCollections(final ClientSession clientSession, final Class<C> clazz) {
        return new ListCollectionsPublisherImpl<>(wrapped.listCollections(clientSession.getWrapped(), clazz));
    }

    @Override
    public Publisher<Void> createCollection(final String collectionName) {
        return createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public Publisher<Void> createCollection(final String collectionName, final CreateCollectionOptions options) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.createCollection(collectionName, options, callback));
    }

    @Override
    public Publisher<Void> createCollection(final ClientSession clientSession, final String collectionName) {
        return createCollection(clientSession, collectionName, new CreateCollectionOptions());
    }

    @Override
    public Publisher<Void> createCollection(final ClientSession clientSession, final String collectionName,
                                               final CreateCollectionOptions options) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.createCollection(clientSession.getWrapped(), collectionName, options, callback));
    }

    @Override
    public Publisher<Void> createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline) {
        return createView(viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public Publisher<Void> createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                                         final CreateViewOptions createViewOptions) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.createView(viewName, viewOn, pipeline, createViewOptions, callback));
    }

    @Override
    public Publisher<Void> createView(final ClientSession clientSession, final String viewName, final String viewOn,
                                         final List<? extends Bson> pipeline) {
        return createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public Publisher<Void> createView(final ClientSession clientSession, final String viewName, final String viewOn,
                                         final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.createView(clientSession.getWrapped(), viewName, viewOn, pipeline, createViewOptions,
                        callback));
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return watch(Collections.emptyList());
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new ChangeStreamPublisherImpl<TResult>(wrapped.watch(pipeline, resultClass));
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return new ChangeStreamPublisherImpl<TResult>(wrapped.watch(clientSession.getWrapped(), pipeline, resultClass));
    }

    @Override
    public AggregatePublisher<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new AggregatePublisherImpl<TResult>(wrapped.aggregate(pipeline, resultClass));
    }

    @Override
    public AggregatePublisher<Document> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                           final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return new AggregatePublisherImpl<>(wrapped.aggregate(clientSession.getWrapped(), pipeline, resultClass));
    }

    /**
     * Gets the wrapped MongoDatabase
     *
     * <p>This should not be considered a part of the public API.</p>
     *
     * @return wrapped MongoDatabase
     */
    public AsyncMongoDatabase getWrapped() {
        return wrapped;
    }
}
