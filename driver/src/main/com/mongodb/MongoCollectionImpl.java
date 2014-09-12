/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.client.CollectionAdministration;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.MongoPipeline;
import com.mongodb.client.MongoView;
import com.mongodb.codecs.CollectibleCodec;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregationOptions;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.Find;
import com.mongodb.operation.FindAndRemove;
import com.mongodb.operation.FindAndRemoveOperation;
import com.mongodb.operation.FindAndReplace;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdate;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.MapReduce;
import com.mongodb.operation.MapReduceToCollectionOperation;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.operation.QueryOperation;
import com.mongodb.operation.QueryOptions;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.RemoveOperation;
import com.mongodb.operation.RemoveRequest;
import com.mongodb.operation.ReplaceOperation;
import com.mongodb.operation.ReplaceRequest;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.UpdateRequest;
import com.mongodb.operation.WriteOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonJavaScript;
import org.bson.codecs.Codec;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import static java.util.Arrays.asList;

class MongoCollectionImpl<T> implements MongoCollection<T> {

    private final CollectionAdministration admin;
    private final MongoClient client;
    private final String name;
    private final MongoDatabase database;
    private final MongoCollectionOptions options;
    private final Codec<T> codec;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final Class<T> clazz, final MongoCollectionOptions options,
                               final MongoClient client) {

        this.codec = options.getCodecRegistry().get(clazz);
        this.name = name;
        this.database = database;
        this.options = options;
        this.client = client;
        admin = new CollectionAdministrationImpl(client, getNamespace());
    }

    @Override
    public WriteResult insert(final T document) {
        return new MongoCollectionView().insert(document);
    }

    @Override
    public WriteResult insert(final List<T> documents) {
        return new MongoCollectionView().insert(documents);
    }

    @Override
    public WriteResult save(final T document) {
        return new MongoCollectionView().save(document);
    }

    @Override
    public MongoPipeline<T> pipe() {
        return new MongoCollectionPipeline();
    }

    @Override
    public CollectionAdministration tools() {
        return admin;
    }

    @Override
    public MongoView<T> find() {
        return new MongoCollectionView();
    }

    @Override
    public MongoView<T> find(final Document filter) {
        return new MongoCollectionView().find(filter);
    }

    @Override
    public MongoView<T> find(final ConvertibleToDocument filter) {
        return new MongoCollectionView().find(filter);
    }

    @Override
    public MongoView<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionView().withWriteConcern(writeConcern);
    }

    private Codec<Document> getDocumentCodec() {
        return getOptions().getCodecRegistry().get(Document.class);
    }

    @Override
    public MongoDatabase getDatabase() {
        return database;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Codec<T> getCodec() {
        return codec;
    }

    @Override
    public MongoCollectionOptions getOptions() {
        return options;
    }

    @Override
    public MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabase().getName(), getName());
    }

    <V> V execute(final ReadOperation<V> operation, final ReadPreference readPreference) {
        return client.execute(operation, readPreference);
    }

    <V> V execute(final WriteOperation<V> operation) {
        return client.execute(operation);
    }

    private BsonDocument wrap(final Document command) {
        return new BsonDocumentWrapper<Document>(command, getDocumentCodec());
    }

    private final class MongoCollectionView implements MongoView<T> {
        private final Find findOp;
        private ReadPreference readPreference;
        private WriteConcern writeConcern;
        private boolean limitSet;
        private boolean upsert;

        private MongoCollectionView() {
            findOp = new Find();
            writeConcern = getOptions().getWriteConcern();
            readPreference = getOptions().getReadPreference();
        }

        @Override
        public MongoCursor<T> iterator() {
            return get();
        }

        @Override
        public MongoView<T> cursorFlags(final  EnumSet<CursorFlag> flags) {
            findOp.addFlags(flags);
            return this;
        }

        @Override
        public MongoView<T> find(final Document filter) {
            findOp.filter(new BsonDocumentWrapper<Document>(filter, getDocumentCodec()));
            return this;
        }

        MongoView<T> find(final BsonDocument filter) {
            findOp.filter(filter);
            return this;
        }

        @Override
        public MongoView<T> find(final ConvertibleToDocument filter) {
            return find(filter.toDocument());
        }

        @Override
        public MongoView<T> sort(final ConvertibleToDocument sortCriteria) {
            return sort(sortCriteria.toDocument());
        }

        @Override
        public MongoView<T> sort(final Document sortCriteria) {
            findOp.order(wrap(sortCriteria));
            return this;
        }

        @Override
        public MongoView<T> fields(final Document selector) {
            findOp.select(wrap(selector));
            return this;
        }

        @Override
        public MongoView<T> fields(final ConvertibleToDocument selector) {
            return fields(selector.toDocument());
        }

        @Override
        public MongoView<T> upsert() {
            upsert = true;
            return this;
        }

        @Override
        public MongoView<T> withQueryOptions(final QueryOptions queryOptions) {
            findOp.options(queryOptions);
            return this;
        }

        @Override
        public MongoView<T> skip(final int skip) {
            findOp.skip(skip);
            return this;
        }

        @Override
        public MongoView<T> limit(final int limit) {
            findOp.limit(limit);
            limitSet = true;
            return this;
        }

        @Override
        public MongoView<T> withReadPreference(final ReadPreference readPreference) {
            this.readPreference = readPreference;
            return this;
        }

        @Override
        public MongoCursor<T> get() {
            return execute(new QueryOperation<T>(getNamespace(), findOp, getCodec()), readPreference);
        }

        @Override
        public T getOne() {
            MongoCursor<T> cursor = execute(new QueryOperation<T>(getNamespace(), findOp.batchSize(-1), getCodec()),
                                            readPreference);

            return cursor.hasNext() ? cursor.next() : null;
        }

        @Override
        public long count() {
            return execute(new CountOperation(getNamespace(), findOp), readPreference);
        }

        @Override
        public MongoIterable<Document> mapReduce(final String map, final String reduce) {
            //TODO: support supplied read preferences?
            MapReduce mapReduce = new MapReduce(new BsonJavaScript(map), new BsonJavaScript(reduce)).filter(findOp.getFilter())
                                                                                .limit(findOp.getLimit());

            if (mapReduce.isInline()) {
                MapReduceWithInlineResultsOperation<Document> operation =
                new MapReduceWithInlineResultsOperation<Document>(getNamespace(), mapReduce, new DocumentCodec());
                return new MapReduceResultsIterable<T, Document>(operation, MongoCollectionImpl.this);
            } else {
                execute(new MapReduceToCollectionOperation(getNamespace(), mapReduce));
                return client.getDatabase(mapReduce.getOutput().getDatabaseName()).getCollection(mapReduce.getOutput().getCollectionName())
                             .find();
            }
        }

        @Override
        public void forEach(final Block<? super T> block) {
            MongoCursor<T> cursor = get();
            try {
                while (cursor.hasNext()) {
                    block.apply(cursor.next());
                }
            } finally {
                cursor.close();
            }
        }


        @Override
        public <A extends Collection<? super T>> A into(final A target) {
            forEach(new Block<T>() {
                @Override
                public void apply(final T t) {
                    target.add(t);
                }
            });
            return target;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<T, U> mapper) {
            return new MappingIterable<T, U>(this, mapper);
        }

        @Override
        public MongoView<T> withWriteConcern(final WriteConcern writeConcernForThisOperation) {
            writeConcern = writeConcernForThisOperation;
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public WriteResult insert(final T document) {
            return execute(new InsertOperation<T>(getNamespace(), true, writeConcern, asList(new InsertRequest<T>(document)), getCodec()));
        }

        @Override
        public WriteResult insert(final List<T> documents) {
            List<InsertRequest<T>> insertRequestList = new ArrayList<InsertRequest<T>>(documents.size());
            for (T cur : documents) {
                insertRequestList.add(new InsertRequest<T>(cur));
            }
            return execute(new InsertOperation<T>(getNamespace(), true, writeConcern, insertRequestList, getCodec()));
        }

        @Override
        public WriteResult save(final T document) {
            if (!(codec instanceof CollectibleCodec)) {
                throw new UnsupportedOperationException();
            }
            CollectibleCodec<T> collectibleCodec = (CollectibleCodec<T>) codec;
            if (!collectibleCodec.documentHasId(document)) {
                return insert(document);
            } else {
                return find(new BsonDocument("_id", collectibleCodec.getDocumentId(document))).upsert().replace(document);
            }
        }

        @Override
        public WriteResult remove() {
            RemoveRequest removeRequest = new RemoveRequest(findOp.getFilter()).multi(getMultiFromLimit());
            return execute(new RemoveOperation(getNamespace(), true, writeConcern, asList(removeRequest)
            ));
        }

        @Override
        public WriteResult removeOne() {
            RemoveRequest removeRequest = new RemoveRequest(findOp.getFilter()).multi(false);
            return execute(new RemoveOperation(getNamespace(), true, writeConcern, asList(removeRequest)
            ));
        }

        @Override
        public WriteResult update(final Document updateOperations) {
            UpdateRequest update = new UpdateRequest(findOp.getFilter(), wrap(updateOperations)).upsert(upsert)
                                                                                                .multi(getMultiFromLimit());
            return execute(new UpdateOperation(getNamespace(), true, writeConcern, asList(update)
            ));
        }

        @Override
        public WriteResult update(final ConvertibleToDocument updateOperations) {
            return update(updateOperations.toDocument());
        }

        @Override
        public WriteResult updateOne(final Document updateOperations) {
            UpdateRequest update = new UpdateRequest(findOp.getFilter(), wrap(updateOperations)).upsert(upsert).multi(false);
            return execute(new UpdateOperation(getNamespace(), true, writeConcern, asList(update)));
        }

        @Override
        public WriteResult updateOne(final ConvertibleToDocument updateOperations) {
            return updateOne(updateOperations.toDocument());
        }

        @Override
        @SuppressWarnings("unchecked")
        public WriteResult replace(final T replacement) {
            ReplaceRequest<T> replaceRequest = new ReplaceRequest<T>(findOp.getFilter(), replacement).upsert(upsert);
            return execute(new ReplaceOperation<T>(getNamespace(), true, writeConcern, asList(replaceRequest),
                                                   getCodec()));
        }

        @Override
        public T updateOneAndGet(final Document updateOperations) {
            return updateOneAndGet(updateOperations, Get.AfterChangeApplied);
        }

        @Override
        public T updateOneAndGet(final ConvertibleToDocument updateOperations) {
            return updateOneAndGet(updateOperations.toDocument());
        }

        @Override
        public T replaceOneAndGet(final T replacement) {
            return replaceOneAndGet(replacement, Get.AfterChangeApplied);
        }

        @Override
        public T getOneAndUpdate(final Document updateOperations) {
            return updateOneAndGet(updateOperations, Get.BeforeChangeApplied);
        }

        @Override
        public T getOneAndUpdate(final ConvertibleToDocument updateOperations) {
            return getOneAndUpdate(updateOperations.toDocument());
        }

        @Override
        public T getOneAndReplace(final T replacement) {
            return replaceOneAndGet(replacement, Get.BeforeChangeApplied);
        }

        public T updateOneAndGet(final Document updateOperations, final Get beforeOrAfter) {
            FindAndUpdate findAndUpdate = new FindAndUpdate().where(findOp.getFilter())
                                                             .updateWith(wrap(updateOperations))
                                                             .returnNew(asBoolean(beforeOrAfter))
                                                             .select(findOp.getFields())
                                                             .sortBy(findOp.getOrder())
                                                             .upsert(upsert);

            return execute(new FindAndUpdateOperation<T>(getNamespace(), findAndUpdate, getCodec()));
        }

        public T replaceOneAndGet(final T replacement, final Get beforeOrAfter) {
            FindAndReplace<T> findAndReplace = new FindAndReplace<T>(replacement).where(findOp.getFilter())
                                                                                 .returnNew(asBoolean(beforeOrAfter))
                                                                                 .select(findOp.getFields())
                                                                                 .sortBy(findOp.getOrder())
                                                                                 .upsert(upsert);
            return execute(new FindAndReplaceOperation<T>(getNamespace(), findAndReplace, getCodec()));
        }

        @Override
        public T getOneAndRemove() {
            FindAndRemove<T> findAndRemove = new FindAndRemove<T>().where(findOp.getFilter())
                                                                   .select(findOp.getFields())
                                                                   .sortBy(findOp.getOrder());

            return execute(new FindAndRemoveOperation<T>(getNamespace(), findAndRemove, getCodec()));
        }

        boolean asBoolean(final Get get) {
            return get == Get.AfterChangeApplied;
        }

        private boolean getMultiFromLimit() {
            if (limitSet) {
                if (findOp.getLimit() == 1) {
                    return false;
                } else if (findOp.getLimit() == 0) {
                    return true;
                } else {
                    throw new IllegalArgumentException("Update currently only supports a limit of either none or 1");
                }
            } else {
                return true;
            }
        }
    }

    private class MongoCollectionPipeline implements MongoPipeline<T> {
        private final List<BsonDocument> pipeline;

        private MongoCollectionPipeline() {
            pipeline = new ArrayList<BsonDocument>();
        }

        public MongoCollectionPipeline(final MongoCollectionPipeline from) {
            pipeline = new ArrayList<BsonDocument>(from.pipeline);
        }

        @Override
        public MongoPipeline<T> find(final Document criteria) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(wrap(new Document("$match", criteria)));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> sort(final Document sortCriteria) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(wrap(new Document("$sort", sortCriteria)));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> skip(final long skip) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(wrap(new Document("$skip", skip)));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> limit(final long limit) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(wrap(new Document("$limit", limit)));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> project(final Document projection) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(wrap(new Document("$project", projection)));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> group(final Document group) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(wrap(new Document("$group", group)));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> unwind(final String field) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(wrap(new Document("$unwind", field)));
            return newPipeline;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<T, U> mapper) {
            return new MappingIterable<T, U>(this, mapper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public MongoCursor<T> iterator() {
            return execute(new AggregateOperation<T>(getNamespace(), pipeline, getCodec(), AggregationOptions.builder().build()),
                           options.getReadPreference());
        }

        @Override
        public void forEach(final Block<? super T> block) {
            MongoCursor<T> cursor = iterator();
            try {
                while (cursor.hasNext()) {
                    block.apply(cursor.next());
                }
            } finally {
                cursor.close();
            }
        }

        @Override
        public <A extends Collection<? super T>> A into(final A target) {
            forEach(new Block<T>() {
                @Override
                public void apply(final T t) {
                    target.add(t);
                }
            });
            return target;
        }
    }
}
