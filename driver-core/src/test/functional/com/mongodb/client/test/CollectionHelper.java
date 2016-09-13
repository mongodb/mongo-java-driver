/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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
import com.mongodb.WriteConcern;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.bulk.IndexRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.CreateIndexesOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.MixedBulkWriteOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.ValueCodecProvider;
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
    private CodecRegistry registry = CodecRegistries.fromProviders(new BsonValueCodecProvider(),
                                                                   new IterableCodecProvider(),
                                                                   new ValueCodecProvider(),
                                                                   new DocumentCodecProvider(),
                                                                   new GeoJsonCodecProvider());
    private MongoNamespace namespace;

    public CollectionHelper(final Codec<T> codec, final MongoNamespace namespace) {
        this.codec = codec;
        this.namespace = namespace;
    }

    public static void drop(final MongoNamespace namespace) {
        new DropCollectionOperation(namespace, WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

    public static void dropDatabase(final String name) {
        if (name == null) {
            return;
        }
        try {
            new DropDatabaseOperation(name, WriteConcern.ACKNOWLEDGED).execute(getBinding());
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    public void drop() {
        new DropCollectionOperation(namespace, WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

    public void create(final String collectionName, final CreateCollectionOptions options) {
        drop(namespace);
        CreateCollectionOperation operation = new CreateCollectionOperation(namespace.getDatabaseName(), collectionName,
                                                                                   WriteConcern.ACKNOWLEDGED)
                .capped(options.isCapped())
                .sizeInBytes(options.getSizeInBytes())
                .autoIndex(options.isAutoIndex())
                .maxDocuments(options.getMaxDocuments())
                .usePowerOf2Sizes(options.isUsePowerOf2Sizes());

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

    @SuppressWarnings("unchecked")
    public void insertDocuments(final BsonDocument... documents) {
        for (BsonDocument document : documents) {
            new InsertOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                asList(new InsertRequest(document))).execute(getBinding());
        }
    }

    @SuppressWarnings("unchecked")
    public void insertDocuments(final List<BsonDocument> documents) {
        insertDocuments(documents, getBinding());
    }


    @SuppressWarnings("unchecked")
    public void insertDocuments(final List<BsonDocument> documents, final WriteBinding binding) {
        insertDocuments(documents, WriteConcern.ACKNOWLEDGED, binding);
    }

    @SuppressWarnings("unchecked")
    public void insertDocuments(final List<BsonDocument> documents, final WriteConcern writeConcern, final WriteBinding binding) {
        for (BsonDocument document : documents) {
            new InsertOperation(namespace, true, writeConcern,
                                singletonList(new InsertRequest(document))).execute(binding);
        }
    }

    public void insertDocuments(final Document... documents) {
        insertDocuments(new DocumentCodec(registry, new BsonTypeClassMap()), asList(documents));
    }

    public <I> void insertDocuments(final Codec<I> iCodec, final I... documents) {
        insertDocuments(iCodec, asList(documents));
    }

    public <I> void insertDocuments(final Codec<I> iCodec, final WriteBinding binding, final I... documents) {
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
        BatchCursor<D> cursor = new FindOperation<D>(namespace, codec).execute(getBinding());
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
                                    true, WriteConcern.ACKNOWLEDGED)
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

    public List<T> find(final Bson filter, final Bson sort) {
        return find(filter != null ? filter.toBsonDocument(Document.class, registry) : null,
                    sort != null ? sort.toBsonDocument(Document.class, registry) : null,
                    codec);
    }

    public List<T> find(final Bson filter, final Bson sort, final Bson projection) {
        return find(filter != null ? filter.toBsonDocument(Document.class, registry) : null,
                    sort != null ? sort.toBsonDocument(Document.class, registry) : null,
                    projection != null ? projection.toBsonDocument(Document.class, registry) : null,
                    codec);
    }

    public <D> List<D> find(final BsonDocument filter, final Decoder<D> decoder) {
        return find(filter, null, decoder);
    }

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
        return new CountOperation(namespace).execute(binding);
    }

    public long count(final AsyncReadWriteBinding binding) throws Throwable {
        return executeAsync(new CountOperation(namespace), binding);
    }

    public long count(final Bson filter) {
        return new CountOperation(namespace).filter(toBsonDocument(filter)).execute(getBinding());
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

    public void createIndex(final Document key, final String defaultLanguage) {
        new CreateIndexesOperation(namespace, asList(new IndexRequest(wrap(key)).defaultLanguage(defaultLanguage)),
                                          WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

    public void createIndex(final Bson key) {
        new CreateIndexesOperation(namespace, asList(new IndexRequest(key.toBsonDocument(Document.class, registry))),
                                          WriteConcern.ACKNOWLEDGED).execute(getBinding());
    }

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
}
