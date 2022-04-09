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

package com.mongodb.client.test;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerCursor;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.operation.AggregateOperation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.internal.operation.CountDocumentsOperation;
import com.mongodb.internal.operation.CreateCollectionOperation;
import com.mongodb.internal.operation.CreateIndexesOperation;
import com.mongodb.internal.operation.DropCollectionOperation;
import com.mongodb.internal.operation.DropDatabaseOperation;
import com.mongodb.internal.operation.FindOperation;
import com.mongodb.internal.operation.InsertOperation;
import com.mongodb.internal.operation.ListIndexesOperation;
import com.mongodb.internal.operation.MixedBulkWriteOperation;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ClusterFixture.executeAsync;
import static com.mongodb.ClusterFixture.getBinding;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class CollectionHelper<T> {

    private Codec<T> codec;
    private CodecRegistry registry = CodecRegistries.fromProviders(Bson.DEFAULT_CODEC_REGISTRY, new GeoJsonCodecProvider());
    private MongoNamespace namespace;

    public CollectionHelper(final Codec<T> codec, final MongoNamespace namespace) {
        this.codec = codec;
        this.namespace = namespace;
    }

    public T hello() {
        return new CommandReadOperation<T>("admin", BsonDocument.parse("{isMaster: 1}"), codec).execute(getBinding());
    }

    public static void drop(final MongoNamespace namespace) {
        drop(namespace, WriteConcern.ACKNOWLEDGED);
    }

    public static void drop(final MongoNamespace namespace, final WriteConcern writeConcern) {
        new DropCollectionOperation(namespace, writeConcern).execute(getBinding());
    }

    public static void dropDatabase(final String name) {
        dropDatabase(name, WriteConcern.ACKNOWLEDGED);
    }

    public static void dropDatabase(final String name, final WriteConcern writeConcern) {
        if (name == null) {
            return;
        }
        try {
            new DropDatabaseOperation(name, writeConcern).execute(getBinding());
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public void drop() {
        drop(WriteConcern.ACKNOWLEDGED);
    }

    public void drop(final WriteConcern writeConcern) {
        drop(namespace, writeConcern);
    }

    public void create() {
        create(namespace.getCollectionName(), new CreateCollectionOptions(), WriteConcern.ACKNOWLEDGED);
    }

    public void create(final WriteConcern writeConcern) {
        create(namespace.getCollectionName(), new CreateCollectionOptions(), writeConcern);
    }

    public void create(final String collectionName, final CreateCollectionOptions options) {
        create(collectionName, options, WriteConcern.ACKNOWLEDGED);
    }

    public void create(final String collectionName, final CreateCollectionOptions options, final WriteConcern writeConcern) {
        drop(namespace, writeConcern);
        CreateCollectionOperation operation = new CreateCollectionOperation(namespace.getDatabaseName(), collectionName, writeConcern)
                .capped(options.isCapped())
                .sizeInBytes(options.getSizeInBytes())
                .maxDocuments(options.getMaxDocuments());

        IndexOptionDefaults indexOptionDefaults = options.getIndexOptionDefaults();
        if (indexOptionDefaults.getStorageEngine() != null) {
            operation.indexOptionDefaults(new BsonDocument("storageEngine", toBsonDocument(indexOptionDefaults.getStorageEngine())));
        }
        ValidationOptions validationOptions = options.getValidationOptions();
        if (validationOptions.getValidator() != null) {
            operation.validator(toBsonDocument(validationOptions.getValidator()));
        }
        if (validationOptions.getValidationLevel() != null) {
            operation.validationLevel(validationOptions.getValidationLevel());
        }
        if (validationOptions.getValidationAction() != null) {
            operation.validationAction(validationOptions.getValidationAction());
        }
        operation.execute(getBinding());
    }

    public void killCursor(final MongoNamespace namespace, final ServerCursor serverCursor) {
        if (serverCursor != null) {
            BsonDocument command = new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                    .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
            try {
                new CommandReadOperation<>(namespace.getDatabaseName(), command, new BsonDocumentCodec())
                        .execute(getBinding());
            } catch (Exception e) {
                // Ignore any exceptions killing old cursors
            }
        }
    }

    public void insertDocuments(final BsonDocument... documents) {
        insertDocuments(asList(documents));
    }

    public void insertDocuments(final List<BsonDocument> documents) {
        insertDocuments(documents, getBinding());
    }

    public void insertDocuments(final List<BsonDocument> documents, final WriteConcern writeConcern) {
        insertDocuments(documents, writeConcern, getBinding());
    }

    public void insertDocuments(final List<BsonDocument> documents, final WriteBinding binding) {
        insertDocuments(documents, WriteConcern.ACKNOWLEDGED, binding);
    }

    public void insertDocuments(final List<BsonDocument> documents, final WriteConcern writeConcern, final WriteBinding binding) {
        List<InsertRequest> insertRequests = new ArrayList<InsertRequest>(documents.size());
        for (BsonDocument document : documents) {
            insertRequests.add(new InsertRequest(document));
        }
        new InsertOperation(namespace, true, writeConcern, false, insertRequests).execute(binding);
    }

    public void insertDocuments(final Document... documents) {
        insertDocuments(new DocumentCodec(registry), asList(documents));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public final <I> void insertDocuments(final Codec<I> iCodec, final I... documents) {
        insertDocuments(iCodec, asList(documents));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public final <I> void insertDocuments(final Codec<I> iCodec, final WriteBinding binding, final I... documents) {
        insertDocuments(iCodec, binding, asList(documents));
    }

    public <I> void insertDocuments(final Codec<I> iCodec, final List<I> documents) {
        insertDocuments(iCodec, getBinding(), documents);
    }

    public <I> void insertDocuments(final Codec<I> iCodec, final WriteBinding binding, final List<I> documents) {
        List<BsonDocument> bsonDocuments = new ArrayList<BsonDocument>(documents.size());
        for (I document : documents) {
            bsonDocuments.add(new BsonDocumentWrapper<I>(document, iCodec));
        }
        insertDocuments(bsonDocuments, binding);
    }

    public List<T> find() {
        return find(codec);
    }

    public <D> List<D> find(final Codec<D> codec) {
        BatchCursor<D> cursor = new FindOperation<D>(namespace, codec)
                .sort(new BsonDocument("_id", new BsonInt32(1)))
                .execute(getBinding());
        List<D> results = new ArrayList<D>();
        while (cursor.hasNext()) {
            results.addAll(cursor.next());
        }
        return results;
    }

    public void updateOne(final Bson filter, final Bson update) {
        updateOne(filter, update, false);
    }

    public void updateOne(final Bson filter, final Bson update, final boolean isUpsert) {
        new MixedBulkWriteOperation(namespace,
                                    singletonList(new UpdateRequest(filter.toBsonDocument(Document.class, registry),
                                                                    update.toBsonDocument(Document.class, registry),
                                                                    WriteRequest.Type.UPDATE)
                                                  .upsert(isUpsert)),
                                    true, WriteConcern.ACKNOWLEDGED, false)
        .execute(getBinding());
    }

    public void replaceOne(final Bson filter, final Bson update, final boolean isUpsert) {
        new MixedBulkWriteOperation(namespace,
                singletonList(new UpdateRequest(filter.toBsonDocument(Document.class, registry),
                        update.toBsonDocument(Document.class, registry),
                        WriteRequest.Type.REPLACE)
                        .upsert(isUpsert)),
                true, WriteConcern.ACKNOWLEDGED, false)
                .execute(getBinding());
    }

    public void deleteOne(final Bson filter) {
        new MixedBulkWriteOperation(namespace,
                singletonList(new DeleteRequest(filter.toBsonDocument(Document.class, registry))),
                true, WriteConcern.ACKNOWLEDGED, false)
                .execute(getBinding());
    }

    public List<T> find(final Bson filter) {
        return find(filter, null);
    }

    public List<T> aggregate(final List<Bson> pipeline) {
        return aggregate(pipeline, codec);
    }

    public <D> List<D> aggregate(final List<Bson> pipeline, final Decoder<D> decoder) {
        List<BsonDocument> bsonDocumentPipeline = new ArrayList<BsonDocument>();
        for (Bson cur : pipeline) {
            bsonDocumentPipeline.add(cur.toBsonDocument(Document.class, registry));
        }
        BatchCursor<D> cursor = new AggregateOperation<D>(namespace, bsonDocumentPipeline, decoder)
                                .execute(getBinding());
        List<D> results = new ArrayList<D>();
        while (cursor.hasNext()) {
            results.addAll(cursor.next());
        }
        return results;
    }

    @SuppressWarnings("overloads")
    public List<T> find(final Bson filter, final Bson sort) {
        return find(filter != null ? filter.toBsonDocument(Document.class, registry) : null,
                    sort != null ? sort.toBsonDocument(Document.class, registry) : null,
                    codec);
    }

    @SuppressWarnings("overloads")
    public List<T> find(final Bson filter, final Bson sort, final Bson projection) {
        return find(filter != null ? filter.toBsonDocument(Document.class, registry) : null,
                    sort != null ? sort.toBsonDocument(Document.class, registry) : null,
                    projection != null ? projection.toBsonDocument(Document.class, registry) : null,
                    codec);
    }

    @SuppressWarnings("overloads")
    public <D> List<D> find(final BsonDocument filter, final Decoder<D> decoder) {
        return find(filter, null, decoder);
    }

    @SuppressWarnings("overloads")
    public <D> List<D> find(final BsonDocument filter, final BsonDocument sort, final Decoder<D> decoder) {
        return find(filter, sort, null, decoder);
    }

    public <D> List<D> find(final BsonDocument filter, final BsonDocument sort, final BsonDocument projection, final Decoder<D> decoder) {
        BatchCursor<D> cursor = new FindOperation<D>(namespace, decoder).filter(filter).sort(sort).projection(projection)
                                                                        .execute(getBinding());
        List<D> results = new ArrayList<D>();
        while (cursor.hasNext()) {
            results.addAll(cursor.next());
        }
        return results;
    }

    public long count() {
        return count(getBinding());
    }

    public long count(final ReadBinding binding) {
        return new CountDocumentsOperation(namespace).execute(binding);
    }

    public long count(final AsyncReadWriteBinding binding) throws Throwable {
        return executeAsync(new CountDocumentsOperation(namespace), binding);
    }

    public long count(final Bson filter) {
        return new CountDocumentsOperation(namespace).filter(toBsonDocument(filter)).execute(getBinding());
    }

    public BsonDocument wrap(final Document document) {
        return new BsonDocumentWrapper<Document>(document, new DocumentCodec());
    }

    public BsonDocument toBsonDocument(final Bson document) {
        return document.toBsonDocument(BsonDocument.class, registry);
    }

    public void createIndex(final BsonDocument key) {
        new CreateIndexesOperation(namespace, asList(new IndexRequest(key)), WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

    public void createIndex(final Document key) {
        new CreateIndexesOperation(namespace, asList(new IndexRequest(wrap(key))), WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

    public void createUniqueIndex(final Document key) {
        new CreateIndexesOperation(namespace, asList(new IndexRequest(wrap(key)).unique(true)), WriteConcern.ACKNOWLEDGED)
                .execute(getBinding());
    }

    public void createIndex(final Document key, final String defaultLanguage) {
        new CreateIndexesOperation(namespace, asList(new IndexRequest(wrap(key)).defaultLanguage(defaultLanguage)),
                                          WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

    public void createIndex(final Bson key) {
        new CreateIndexesOperation(namespace, asList(new IndexRequest(key.toBsonDocument(Document.class, registry))),
                                          WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

    @SuppressWarnings("deprecation")
    public void createIndex(final Bson key, final Double bucketSize) {
        new CreateIndexesOperation(namespace, asList(new IndexRequest(key.toBsonDocument(Document.class, registry))
                .bucketSize(bucketSize)), WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

    public List<BsonDocument> listIndexes(){
        List<BsonDocument> indexes = new ArrayList<BsonDocument>();
        BatchCursor<BsonDocument> cursor = new ListIndexesOperation<BsonDocument>(namespace, new BsonDocumentCodec()).execute(getBinding());
        while (cursor.hasNext()) {
            indexes.addAll(cursor.next());
        }
        return indexes;
    }

    public void killAllSessions() {
        try {
            new CommandReadOperation<>("admin", new BsonDocument("killAllSessions", new BsonArray()),
                    new BsonDocumentCodec()).execute(getBinding());
        } catch (MongoCommandException e) {
            // ignore exception caused by killing the implicit session that the killAllSessions command itself is running in
        }
    }

    public void renameCollection(final MongoNamespace newNamespace) {
        try {
            new CommandReadOperation<>("admin",
                    new BsonDocument("renameCollection", new BsonString(getNamespace().getFullName()))
                                .append("to", new BsonString(newNamespace.getFullName())),
                    new BsonDocumentCodec()).execute(getBinding());
        } catch (MongoCommandException e) {
            // do nothing
        }
    }

    public void runAdminCommand(final String command) {
        runAdminCommand(BsonDocument.parse(command));
    }

    public void runAdminCommand(final BsonDocument command) {
        new CommandReadOperation<>("admin", command, new BsonDocumentCodec()).execute(getBinding());
    }
}
