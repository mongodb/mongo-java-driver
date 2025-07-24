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

package com.mongodb;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.internal.MongoBatchCursorAdapter;
import com.mongodb.client.internal.MongoIterableImpl;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.client.model.DBCollectionCountOptions;
import com.mongodb.client.model.DBCollectionDistinctOptions;
import com.mongodb.client.model.DBCollectionFindAndModifyOptions;
import com.mongodb.client.model.DBCollectionFindOptions;
import com.mongodb.client.model.DBCollectionRemoveOptions;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest.Type;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.operation.AggregateOperation;
import com.mongodb.internal.operation.AggregateToCollectionOperation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.CountOperation;
import com.mongodb.internal.operation.CreateIndexesOperation;
import com.mongodb.internal.operation.DistinctOperation;
import com.mongodb.internal.operation.DropCollectionOperation;
import com.mongodb.internal.operation.DropIndexOperation;
import com.mongodb.internal.operation.FindAndDeleteOperation;
import com.mongodb.internal.operation.FindAndReplaceOperation;
import com.mongodb.internal.operation.FindAndUpdateOperation;
import com.mongodb.internal.operation.ListIndexesOperation;
import com.mongodb.internal.operation.MapReduceBatchCursor;
import com.mongodb.internal.operation.MapReduceStatistics;
import com.mongodb.internal.operation.MapReduceToCollectionOperation;
import com.mongodb.internal.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.internal.operation.MixedBulkWriteOperation;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.internal.operation.RenameCollectionOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.Encoder;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.BulkWriteHelper.translateBulkWriteResult;
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForDelete;
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForInsert;
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForReplace;
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForUpdate;
import static com.mongodb.MongoNamespace.checkCollectionNameValidity;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.TimeoutSettingsHelper.createTimeoutSettings;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.Locks.withLock;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of a database collection.  A typical invocation sequence is thus:
 * <pre>
 * {@code
 * MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
 * DB db = mongoClient.getDB("mydb");
 * DBCollection collection = db.getCollection("test"); }
 * </pre>
 * To get a collection to use, just specify the name of the collection to the getCollection(String collectionName) method:
 * <pre>
 * {@code
 * DBCollection coll = db.getCollection("testCollection"); }
 * </pre>
 * Once you have the collection object, you can insert documents into the collection:
 * <pre>
 * {@code
 * BasicDBObject doc = new BasicDBObject("name", "MongoDB").append("type", "database")
 *                                                         .append("count", 1)
 *                                                         .append("info", new BasicDBObject("x", 203).append("y", 102));
 * coll.insert(doc); }
 * </pre>
 * To show that the document we inserted in the previous step is there, we can do a simple findOne() operation to get the first document in
 * the collection:
 * <pre>
 * {@code
 * DBObject myDoc = coll.findOne();
 * System.out.println(myDoc); }
 * </pre>
 *
 * See {@link MongoClient#getDB(String)} for further information about the effective deprecation of this class.
 *
 * @mongodb.driver.manual reference/glossary/#term-collection Collection
 */
@ThreadSafe
@SuppressWarnings({"rawtypes", "deprecation"})
public class DBCollection {
    /**
     * The name of the field that uniquely identifies each document in a collection.
     */
    public static final String ID_FIELD_NAME = "_id";
    private final String name;
    private final DB database;
    private final OperationExecutor executor;
    private final boolean retryWrites;
    private final boolean retryReads;
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;
    private volatile ReadConcern readConcern;
    private final Lock factoryAndCodecLock = new ReentrantLock();
    private DBEncoderFactory encoderFactory;
    private DBDecoderFactory decoderFactory;
    private volatile DBCollectionObjectFactory objectFactory;
    private volatile CompoundDBObjectCodec objectCodec;


    /**
     * Constructs new {@code DBCollection} instance. This operation not reflected on the server.
     *  @param name          the name of the collection
     * @param database      the database to which this collections belongs to
     */
    DBCollection(final String name, final DB database, final OperationExecutor executor) {
        checkCollectionNameValidity(name);
        this.name = name;
        this.database = database;
        this.executor = executor;
        this.objectFactory = new DBCollectionObjectFactory();
        this.objectCodec = new CompoundDBObjectCodec(getDefaultDBObjectCodec());
        this.retryWrites = database.getMongoClient().getMongoClientOptions().getRetryWrites();
        this.retryReads = database.getMongoClient().getMongoClientOptions().getRetryReads();
    }

    /**
     * Initializes a new collection. No operation is actually performed on the database.
     *
     * @param database database in which to create the collection
     * @param name     the name of the collection
     */
    protected DBCollection(final DB database, final String name) {
        this(name, database, database.getExecutor());
    }

    /**
     * Insert a document into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param document     {@code DBObject} to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final DBObject document, final WriteConcern writeConcern) {
        return insert(asList(document), writeConcern);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added. Collection wide {@code WriteConcern} will be used.
     *
     * @param documents {@code DBObject}'s to be inserted
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final DBObject... documents) {
        return insert(asList(documents), getWriteConcern());
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents    {@code DBObject}'s to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final WriteConcern writeConcern, final DBObject... documents) {
        return insert(documents, writeConcern);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents    {@code DBObject}'s to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final DBObject[] documents, final WriteConcern writeConcern) {
        return insert(asList(documents), writeConcern);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents list of {@code DBObject} to be inserted
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents) {
        return insert(documents, getWriteConcern());
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents     list of {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents, final WriteConcern aWriteConcern) {
        return insert(documents, aWriteConcern, null);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents     {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param encoder       {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final DBObject[] documents, final WriteConcern aWriteConcern, final DBEncoder encoder) {
        return insert(asList(documents), aWriteConcern, encoder);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents     a list of {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param dbEncoder     {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents, final WriteConcern aWriteConcern,
                              @Nullable final DBEncoder dbEncoder) {
        return insert(documents, new InsertOptions().writeConcern(aWriteConcern).dbEncoder(dbEncoder));
    }

    /**
     * <p>Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.</p>
     *
     * <p>If the value of the continueOnError property of the given {@code InsertOptions} is true,
     * that value will override the value of the continueOnError property of the given {@code WriteConcern}. Otherwise,
     * the value of the continueOnError property of the given {@code WriteConcern} will take effect. </p>
     *
     * @param documents     a list of {@code DBObject}'s to be inserted
     * @param insertOptions the options to use for the insert
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents, final InsertOptions insertOptions) {
        WriteConcern writeConcern = insertOptions.getWriteConcern() != null ? insertOptions.getWriteConcern() : getWriteConcern();
        Encoder<DBObject> encoder = toEncoder(insertOptions.getDbEncoder());

        List<InsertRequest> insertRequestList = new ArrayList<>(documents.size());
        for (DBObject cur : documents) {
            if (cur.get(ID_FIELD_NAME) == null) {
                cur.put(ID_FIELD_NAME, new ObjectId());
            }
            insertRequestList.add(new InsertRequest(new BsonDocumentWrapper<>(cur, encoder)));
        }
        return insert(insertRequestList, writeConcern, insertOptions.isContinueOnError(), insertOptions.getBypassDocumentValidation());
    }

    private Encoder<DBObject> toEncoder(@Nullable final DBEncoder dbEncoder) {
        return dbEncoder != null ? new DBEncoderAdapter(dbEncoder) : objectCodec;
    }

    private WriteResult insert(final List<InsertRequest> insertRequestList, final WriteConcern writeConcern,
                               final boolean continueOnError, @Nullable final Boolean bypassDocumentValidation) {
        return executeWriteOperation(createBulkWriteOperationForInsert(getNamespace(),
                !continueOnError, writeConcern, retryWrites, insertRequestList).bypassDocumentValidation(bypassDocumentValidation));
    }

    WriteResult executeWriteOperation(final LegacyMixedBulkWriteOperation operation) {
        return translateWriteResult(executor.execute(operation, getReadConcern()));
    }

    private WriteResult translateWriteResult(final WriteConcernResult writeConcernResult) {
        if (!writeConcernResult.wasAcknowledged()) {
            return WriteResult.unacknowledged();
        }

        return translateWriteResult(writeConcernResult.getCount(), writeConcernResult.isUpdateOfExisting(),
                                    writeConcernResult.getUpsertedId());
    }

    private WriteResult translateWriteResult(final int count, final boolean isUpdateOfExisting, @Nullable final BsonValue upsertedId) {
        Object newUpsertedId = upsertedId == null
                               ? null
                               : getObjectCodec().decode(new BsonDocumentReader(new BsonDocument("_id", upsertedId)),
                                                         DecoderContext.builder().build())
                                                 .get("_id");
        return new WriteResult(count, isUpdateOfExisting, newUpsertedId);
    }

    /**
     * Update an existing document or insert a document depending on the parameter. If the document does not contain an '_id' field, then
     * the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectId value. If
     * the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field:
     * <ul>
     *     <li>If a document does not exist with the specified '_id' value, the method performs an insert with the specified fields in
     *     the document.</li>
     *     <li>If a document exists with the specified '_id' value, the method performs an update,
     *     replacing all field in the existing record with the fields from the document.</li>
     * </ul>
     *
     * @param document {@link DBObject} to save to the collection.
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert or update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/#modify-a-document-with-save-method Save
     */
    public WriteResult save(final DBObject document) {
        return save(document, getWriteConcern());
    }

    /**
     * Update an existing document or insert a document depending on the parameter. If the document does not contain an '_id' field, then
     * the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectId value. If
     * the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field:
     * <ul>
     *     <li>If a document does not exist with the specified '_id' value, the method performs an insert with the specified fields in
     *     the document.</li>
     *     <li>If a document exists with the specified '_id' value, the method performs an update,
     *     replacing all field in the existing record with the fields from the document.</li>
     * </ul>
     *
     * @param document     {@link DBObject} to save to the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert or update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/#modify-a-document-with-save-method Save
     */
    public WriteResult save(final DBObject document, final WriteConcern writeConcern) {
        Object id = document.get(ID_FIELD_NAME);
        if (id == null) {
            return insert(document, writeConcern);
        } else {
            return replaceOrInsert(document, id, writeConcern);
        }
    }

    private WriteResult replaceOrInsert(final DBObject obj, final Object id, final WriteConcern writeConcern) {
        DBObject filter = new BasicDBObject(ID_FIELD_NAME, id);

        UpdateRequest replaceRequest = new UpdateRequest(wrap(filter), wrap(obj, objectCodec),
                                                         Type.REPLACE).upsert(true);

        return executeWriteOperation(createBulkWriteOperationForReplace(getNamespace(), false,
                writeConcern, retryWrites, singletonList(replaceRequest)));
    }

    /**
     * Modify an existing document or documents in collection. The query parameter employs the same query selectors, as used in {@code
     * find()}.
     *
     * @param query         the selection criteria for the update
     * @param update        the modifications to apply
     * @param upsert        when true, inserts a document if no document matches the update query criteria
     * @param multi         when true, updates all documents in the collection that match the update query criteria, otherwise only updates
     *                      one
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern aWriteConcern) {
        return update(query, update, upsert, multi, aWriteConcern, null);
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@code find()}.
     *
     * @param query         the selection criteria for the update
     * @param update        the modifications to apply
     * @param upsert        when true, inserts a document if no document matches the update query criteria
     * @param multi         when true, updates all documents in the collection that match the update query criteria, otherwise only updates
     *                      one
     * @param concern       {@code WriteConcern} to be used during operation
     * @param encoder       {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern concern, @Nullable final DBEncoder encoder) {
        return update(query, update, upsert, multi, concern, null, encoder);
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link DBCollection#find(DBObject)}.
     *
     * @param query       the selection criteria for the update
     * @param update       the modifications to apply
     * @param upsert  when true, inserts a document if no document matches the update query criteria
     * @param multi   when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @param concern {@code WriteConcern} to be used during operation
     * @param bypassDocumentValidation whether to bypass document validation.
     * @param encoder the DBEncoder to use
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     * @since 2.14
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern concern, @Nullable final Boolean bypassDocumentValidation,
                              @Nullable final DBEncoder encoder) {
        return update(query, update, new DBCollectionUpdateOptions().upsert(upsert).multi(multi)
                .writeConcern(concern).bypassDocumentValidation(bypassDocumentValidation).encoder(encoder));
    }

    /**
     * Modify an existing document or documents in collection. The query parameter employs the same query selectors, as used in {@code
     * find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @param upsert when true, inserts a document if no document matches the update query criteria
     * @param multi  when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi) {
        return update(query, update, upsert, multi, getWriteConcern());
    }

    /**
     * Modify an existing document. The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult update(final DBObject query, final DBObject update) {
        return update(query, update, false, false);
    }

    /**
     * Modify documents in collection. The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult updateMulti(final DBObject query, final DBObject update) {
        return update(query, update, false, true);
    }

    /**
     * Modify an existing document or documents in collection.
     *
     * @param query the selection criteria for the update
     * @param update the modifications to apply
     * @param options the options to apply to the update operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     * @since 3.4
     */
    public WriteResult update(final DBObject query, final DBObject update, final DBCollectionUpdateOptions options) {
        notNull("query", query);
        notNull("update", update);
        notNull("options", options);
        WriteConcern optionsWriteConcern = options.getWriteConcern();
        WriteConcern writeConcern = optionsWriteConcern != null ? optionsWriteConcern : getWriteConcern();
        Type updateType = (!update.keySet().isEmpty() && update.keySet().iterator().next().startsWith("$"))
                ? UPDATE : Type.REPLACE;
        UpdateRequest updateRequest = new UpdateRequest(wrap(query), wrap(update, options.getEncoder()), updateType)
                                              .upsert(options.isUpsert()).multi(options.isMulti())
                                              .collation(options.getCollation())
                                              .arrayFilters(wrapAllowNull(options.getArrayFilters(), options.getEncoder()));
        LegacyMixedBulkWriteOperation operation = (updateType == UPDATE
                ? createBulkWriteOperationForUpdate(getNamespace(), true, writeConcern, retryWrites,
                singletonList(updateRequest))
                : createBulkWriteOperationForReplace(getNamespace(), true, writeConcern, retryWrites,
                singletonList(updateRequest)))
                .bypassDocumentValidation(options.getBypassDocumentValidation());
        return executeWriteOperation(operation);
    }

    /**
     * Remove documents from a collection.
     *
     * @param query the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents
     *              in the collection.
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     */
    public WriteResult remove(final DBObject query) {
        return remove(query, getWriteConcern());
    }

    /**
     * Remove documents from a collection.
     *
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                     documents in the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     */
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern) {
        return remove(query, new DBCollectionRemoveOptions().writeConcern(writeConcern));
    }

    /**
     * Remove documents from a collection.
     *
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                     documents in the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @param encoder      {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     */
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern, final DBEncoder encoder) {
        return remove(query, new DBCollectionRemoveOptions().writeConcern(writeConcern).encoder(encoder));
    }

    /**
     * Remove documents from a collection.
     *
     * @param query   the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                documents in the collection.
     * @param options the options to apply to the delete operation
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     * @since 3.4
     */
    public WriteResult remove(final DBObject query, final DBCollectionRemoveOptions options) {
        notNull("query", query);
        notNull("options", options);
        WriteConcern optionsWriteConcern = options.getWriteConcern();
        WriteConcern writeConcern = optionsWriteConcern != null ? optionsWriteConcern : getWriteConcern();
        DeleteRequest deleteRequest = new DeleteRequest(wrap(query, options.getEncoder())).collation(options.getCollation());
        return executeWriteOperation(createBulkWriteOperationForDelete(getNamespace(), false,
                writeConcern, retryWrites, singletonList(deleteRequest)));
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query the selection criteria using query operators. Omit the query parameter or pass an empty document to return all documents
     *              in the collection.
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    public DBCursor find(final DBObject query) {
        return new DBCursor(this, query, null, getReadPreference());
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query      the selection criteria using query operators. Omit the query parameter or pass an empty document to return all
     *                   documents in the collection.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    public DBCursor find(final DBObject query, final DBObject projection) {
        return new DBCursor(this, query, projection, getReadPreference());
    }

    /**
     * Select all documents in collection and get a cursor to the selected documents.
     *
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    public DBCursor find() {
        return find(new BasicDBObject());
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query         the selection criteria using query operators. Omit the query parameter or pass an empty document to return all
     *                      documents in the collection.
     * @param options       the options for the find operation.
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     * @since 3.4
     */
    public DBCursor find(@Nullable final DBObject query, final DBCollectionFindOptions options) {
        return new DBCursor(this, query, options);
    }

    /**
     * Get a single document from collection.
     *
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne() {
        return findOne(new BasicDBObject());
    }

    /**
     * Get a single document from collection.
     *
     * @param query the selection criteria using query operators.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final DBObject query) {
        return findOne(query, null, null, getReadPreference());
    }

    /**
     * Get a single document from collection.
     *
     * @param query      the selection criteria using query operators.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final DBObject query, final DBObject projection) {
        return findOne(query, projection, null, getReadPreference());
    }

    /**
     * Get a single document from collection.
     *
     * @param query      the selection criteria using query operators.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @param sort       A document whose fields specify the attributes on which to sort the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final DBObject query, final DBObject projection, final DBObject sort) {
        return findOne(query, projection, sort, getReadPreference());
    }

    /**
     * Get a single document from collection.
     *
     * @param query          the selection criteria using query operators.
     * @param projection     specifies which fields MongoDB will return from the documents in the result set.
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final DBObject query, final DBObject projection, final ReadPreference readPreference) {
        return findOne(query, projection, null, readPreference);
    }

    /**
     * Get a single document from collection.
     *
     * @param query          the selection criteria using query operators.
     * @param projection     specifies which projection MongoDB will return from the documents in the result set.
     * @param sort           A document whose fields specify the attributes on which to sort the result set.
     * @param readPreference {@code ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(@Nullable final DBObject query, @Nullable final DBObject projection, @Nullable final DBObject sort,
                            final ReadPreference readPreference) {
        return findOne(query != null ? query : new BasicDBObject(),
                new DBCollectionFindOptions().projection(projection).sort(sort).readPreference(readPreference));
    }

    /**
     * Get a single document from collection by '_id'.
     *
     * @param id value of '_id' field of a document we are looking for
     * @return A document with '_id' provided as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final Object id) {
        return findOne(new BasicDBObject("_id", id), new DBCollectionFindOptions());
    }

    /**
     * Get a single document from collection by '_id'.
     *
     * @param id         value of '_id' field of a document we are looking for
     * @param projection specifies which projection MongoDB will return from the documents in the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final Object id, final DBObject projection) {
        return findOne(new BasicDBObject("_id", id), new DBCollectionFindOptions().projection(projection));
    }

    /**
     * Get a single document from collection.
     *
     * @param query          the selection criteria using query operators.
     * @param findOptions    the options for the find operation.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     * @since 3.4
     */
    @Nullable
    public DBObject findOne(@Nullable final DBObject query, final DBCollectionFindOptions findOptions) {
        return find(query, findOptions).one();
    }

    /**
     * Same as {@link #getCount()}
     *
     * @return the number of documents in collection
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count() {
        return getCount(new BasicDBObject(), new DBCollectionCountOptions());
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count(@Nullable final DBObject query) {
        return getCount(query, new DBCollectionCountOptions());
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query          specifies the selection criteria
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count(@Nullable final DBObject query, final ReadPreference readPreference) {
        return getCount(query, new DBCollectionCountOptions().readPreference(readPreference));
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query     specifies the selection criteria
     * @param options   the options for the count operation.
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     * @since 3.4
     */
    public long count(@Nullable final DBObject query, final DBCollectionCountOptions options) {
        return getCount(query, options);
    }

    /**
     * Get the count of documents in collection.
     *
     * @return the number of documents in collection
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount() {
        return getCount(new BasicDBObject(), new DBCollectionCountOptions());
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(@Nullable final DBObject query) {
        return getCount(query, new DBCollectionCountOptions());
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query     specifies the selection criteria
     * @param options   the options for the count operation.
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     * @since 3.4
     */
    public long getCount(@Nullable final DBObject query, final DBCollectionCountOptions options) {
        notNull("countOptions", options);
        CountOperation operation = new CountOperation(
                getNamespace())
                .skip(options.getSkip())
                .limit(options.getLimit())
                .collation(options.getCollation())
                .retryReads(retryReads);
        if (query != null) {
            operation.filter(wrap(query));
        }
        DBObject hint = options.getHint();
        if (hint != null) {
            operation.hint(wrap(hint));
        } else {
            String hintString = options.getHintString();
            if (hintString != null) {
                operation.hint(new BsonString(hintString));
            }
        }
        ReadPreference optionsReadPreference = options.getReadPreference();
        ReadConcern optionsReadConcern = options.getReadConcern();
        return getExecutor(createTimeoutSettings(getTimeoutSettings(), options))
                .execute(operation, optionsReadPreference != null ? optionsReadPreference : getReadPreference(),
                        optionsReadConcern != null ? optionsReadConcern : getReadConcern(), null);
    }

    /**
     * Change the name of an existing collection.
     *
     * @param newName specifies the new name of the collection
     * @return the collection with new name
     * @throws MongoException if newName is the name of an existing collection.
     * @mongodb.driver.manual reference/command/renameCollection/ Rename Collection
     */
    public DBCollection rename(final String newName) {
        return rename(newName, false);
    }

    /**
     * Change the name of an existing collection.
     *
     * @param newName    specifies the new name of the collection
     * @param dropTarget If {@code true}, mongod will drop the collection with the target name if it exists
     * @return the collection with new name
     * @throws MongoException if target is the name of an existing collection and {@code dropTarget=false}.
     * @mongodb.driver.manual reference/command/renameCollection/ Rename Collection
     */
    public DBCollection rename(final String newName, final boolean dropTarget) {
        try {
            executor.execute(new RenameCollectionOperation(getNamespace(),
                    new MongoNamespace(getNamespace().getDatabaseName(), newName), getWriteConcern())
                    .dropTarget(dropTarget), getReadConcern());
            return getDB().getCollection(newName);
        } catch (MongoWriteConcernException e) {
            throw createWriteConcernException(e);
        }
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName Specifies the field for which to return the distinct values.
     * @return a List of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct(final String fieldName) {
        return distinct(fieldName, getReadPreference());
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName      Specifies the field for which to return the distinct values
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return a List of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct(final String fieldName, final ReadPreference readPreference) {
        return distinct(fieldName, new BasicDBObject(), readPreference);
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName Specifies the field for which to return the distinct values
     * @param query     specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @return an array of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct(final String fieldName, final DBObject query) {
        return distinct(fieldName, query, getReadPreference());
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName      Specifies the field for which to return the distinct values
     * @param query          specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return A {@code List} of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct(final String fieldName, final DBObject query, final ReadPreference readPreference) {
        return distinct(fieldName, new DBCollectionDistinctOptions().filter(query).readPreference(readPreference));
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName  Specifies the field for which to return the distinct values
     * @param options    the options to apply for this operation
     * @return A {@code List} of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     * @since 3.4
     */
    public List distinct(final String fieldName, final DBCollectionDistinctOptions options) {
        notNull("fieldName", fieldName);
        return new MongoIterableImpl<BsonValue>(null, executor,
                                                options.getReadConcern() != null ? options.getReadConcern() : getReadConcern(),
                                                options.getReadPreference() != null ? options.getReadPreference() : getReadPreference(),
                                                retryReads, DBCollection.this.getTimeoutSettings()) {
            @Override
            public ReadOperationCursor<BsonValue> asReadOperation() {
                return new DistinctOperation<>(getNamespace(), fieldName, new BsonValueCodec())
                               .filter(wrapAllowNull(options.getFilter()))
                               .collation(options.getCollation())
                               .retryReads(retryReads);
            }

            @Override
            protected OperationExecutor getExecutor() {
                return executor;
            }

        }.map(bsonValue -> {
            if (bsonValue == null) {
                return null;
            }
            BsonDocument document = new BsonDocument("value", bsonValue);
            DBObject obj = getDefaultDBObjectCodec().decode(new BsonDocumentReader(document), DecoderContext.builder().build());
            return obj.get("value");
        }).into(new ArrayList<>());
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection.
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation.
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return a MapReduceOutput which contains the results of this map reduce operation
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final DBObject query) {
        MapReduceCommand command = new MapReduceCommand(this, map, reduce, outputTarget, MapReduceCommand.OutputType.REDUCE, query);
        return mapReduce(command);
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation.
     * @param outputType   specifies the type of job output
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return a MapReduceOutput which contains the results of this map reduce operation
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final MapReduceCommand.OutputType outputType, final DBObject query) {
        MapReduceCommand command = new MapReduceCommand(this, map, reduce, outputTarget, outputType, query);
        return mapReduce(command);
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     *
     * @param map            a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce         a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget   specifies the location of the result of the map-reduce operation.
     * @param outputType     specifies the type of job output
     * @param query          specifies the selection criteria using query operators for determining the documents input to the map
     *                       function.
     * @param readPreference the read preference specifying where to run the query.  Only applied for Inline output type
     * @return a MapReduceOutput which contains the results of this map reduce operation
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final MapReduceCommand.OutputType outputType, final DBObject query,
                                     final ReadPreference readPreference) {
        MapReduceCommand command = new MapReduceCommand(this, map, reduce, outputTarget, outputType, query);
        command.setReadPreference(readPreference);
        return mapReduce(command);
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection.
     *
     * @param command specifies the details of the Map Reduce operation to perform
     * @return a MapReduceOutput containing the results of the map reduce operation
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(final MapReduceCommand command) {
        ReadPreference readPreference = command.getReadPreference() == null ? getReadPreference() : command.getReadPreference();
        Map<String, Object> scope = command.getScope();
        Boolean jsMode = command.getJsMode();
        if (command.getOutputType() == MapReduceCommand.OutputType.INLINE) {

            MapReduceWithInlineResultsOperation<DBObject> operation = new MapReduceWithInlineResultsOperation<>(
                    getNamespace(), new BsonJavaScript(command.getMap()),
                    new BsonJavaScript(command.getReduce()), getDefaultDBObjectCodec())
                    .filter(wrapAllowNull(command.getQuery()))
                    .limit(command.getLimit())
                    .jsMode(jsMode != null && jsMode)
                    .sort(wrapAllowNull(command.getSort()))
                    .verbose(command.isVerbose())
                    .collation(command.getCollation());

            if (scope != null) {
                operation.scope(wrap(new BasicDBObject(scope)));
            }
            if (command.getFinalize() != null) {
                operation.finalizeFunction(new BsonJavaScript(command.getFinalize()));
            }
            MapReduceBatchCursor<DBObject> executionResult =
                    getExecutor(createTimeoutSettings(getTimeoutSettings(), command))
                    .execute(operation, readPreference, getReadConcern(), null);
            return new MapReduceOutput(command.toDBObject(), executionResult);
        } else {
            String action;
            switch (command.getOutputType()) {
                case REPLACE:
                    action = "replace";
                    break;
                case MERGE:
                    action = "merge";
                    break;
                case REDUCE:
                    action = "reduce";
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected output type");
            }

            MapReduceToCollectionOperation operation =
                new MapReduceToCollectionOperation(
                        getNamespace(), new BsonJavaScript(command.getMap()), new BsonJavaScript(command.getReduce()),
                        command.getOutputTarget(), getWriteConcern())
                    .filter(wrapAllowNull(command.getQuery()))
                    .limit(command.getLimit())
                    .jsMode(jsMode != null && jsMode)
                    .sort(wrapAllowNull(command.getSort()))
                    .verbose(command.isVerbose())
                    .action(action)
                    .databaseName(command.getOutputDB())
                    .bypassDocumentValidation(command.getBypassDocumentValidation())
                    .collation(command.getCollation());

            if (scope != null) {
                operation.scope(wrap(new BasicDBObject(scope)));
            }
            if (command.getFinalize() != null) {
                operation.finalizeFunction(new BsonJavaScript(command.getFinalize()));
            }
            try {
                MapReduceStatistics mapReduceStatistics = executor.execute(operation, getReadConcern());
                DBCollection mapReduceOutputCollection = getMapReduceOutputCollection(command);
                DBCursor executionResult = mapReduceOutputCollection.find();
                return new MapReduceOutput(command.toDBObject(), executionResult, mapReduceStatistics, mapReduceOutputCollection);
            } catch (MongoWriteConcernException e) {
                throw createWriteConcernException(e);
            }
        }
    }

    private DBCollection getMapReduceOutputCollection(final MapReduceCommand command) {
        String requestedDatabaseName = command.getOutputDB();
        DB database = requestedDatabaseName != null
                      ? getDB().getSisterDB(requestedDatabaseName)
                      : getDB();
        return database.getCollection(command.getOutputTargetNonNull());
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline operations to be performed in the aggregation pipeline
     * @param options  options to apply to the aggregation
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    public Cursor aggregate(final List<? extends DBObject> pipeline, final AggregationOptions options) {
        return aggregate(pipeline, options, getReadPreference());
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline       operations to be performed in the aggregation pipeline
     * @param options        options to apply to the aggregation
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    public Cursor aggregate(final List<? extends DBObject> pipeline, final AggregationOptions options,
                            final ReadPreference readPreference) {
        Cursor result;
        notNull("options", options);
        List<BsonDocument> stages = preparePipeline(pipeline);

        BsonValue outCollection = stages.get(stages.size() - 1).get("$out");

        if (outCollection != null) {
            AggregateToCollectionOperation operation =
                    new AggregateToCollectionOperation(
                            getNamespace(), stages, getReadConcern(), getWriteConcern())
                            .allowDiskUse(options.getAllowDiskUse())
                            .bypassDocumentValidation(options.getBypassDocumentValidation())
                            .collation(options.getCollation());
            try {
               getExecutor(createTimeoutSettings(getTimeoutSettings(), options))
                        .execute(operation, getReadPreference(), getReadConcern(), null);
                result = new DBCursor(database.getCollection(outCollection.asString().getValue()), new BasicDBObject(),
                        new DBCollectionFindOptions().readPreference(primary()).collation(options.getCollation()));
            } catch (MongoWriteConcernException e) {
                throw createWriteConcernException(e);
            }
        } else {
            AggregateOperation<DBObject> operation = new AggregateOperation<>(
                    getNamespace(), stages,
                    getDefaultDBObjectCodec())
                    .allowDiskUse(options.getAllowDiskUse())
                    .batchSize(options.getBatchSize())
                    .collation(options.getCollation())
                    .retryReads(retryReads);
            BatchCursor<DBObject> cursor1 =
                   getExecutor(createTimeoutSettings(getTimeoutSettings(), options))
                            .execute(operation, readPreference, getReadConcern(), null);
            result = new MongoCursorAdapter(new MongoBatchCursorAdapter<>(cursor1));
        }
        return result;
    }

    /**
     * Return the explain plan for the aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline to explain
     * @param options  the options to apply to the aggregation
     * @return the command result.  The explain output may change from release to release, so best to simply log this.
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.driver.manual reference/operator/meta/explain/ Explain query
     * @mongodb.server.release 3.6
     */
    public CommandResult explainAggregate(final List<? extends DBObject> pipeline, final AggregationOptions options) {
        AggregateOperation<BsonDocument> operation = new AggregateOperation<>(
                getNamespace(),
                preparePipeline(pipeline), new BsonDocumentCodec())
                .allowDiskUse(options.getAllowDiskUse())
                .collation(options.getCollation())
                .retryReads(retryReads);
        return new CommandResult(executor.execute(
                operation.asExplainableOperation(ExplainVerbosity.QUERY_PLANNER, new BsonDocumentCodec()), primaryPreferred(), getReadConcern(), null), getDefaultDBObjectCodec());
    }

    List<BsonDocument> preparePipeline(final List<? extends DBObject> pipeline) {
        List<BsonDocument> stages = new ArrayList<>();
        for (final DBObject op : pipeline) {
            stages.add(wrap(op));
        }

        return stages;
    }

    /**
     * Get the name of a collection.
     *
     * @return the name of a collection
     */
    public String getName() {
        return name;
    }

    /**
     * Get the full name of a collection, with the database name as a prefix.
     *
     * @return the name of a collection
     * @mongodb.driver.manual reference/glossary/#term-namespace Namespace
     */
    public String getFullName() {
        return getNamespace().getFullName();
    }

    /**
     * Find a collection that is prefixed with this collection's name. A typical use of this might be
     * <pre>{@code
     *    DBCollection users = mongo.getCollection( "wiki" ).getCollection( "users" );
     * }</pre>
     * Which is equivalent to
     * <pre>{@code
     *   DBCollection users = mongo.getCollection( "wiki.users" );
     * }</pre>
     *
     * @param name the name of the collection to find
     * @return the matching collection
     */
    public DBCollection getCollection(final String name) {
        return database.getCollection(getName() + "." + name);
    }

    /**
     * Forces creation of an ascending index on a field with the default options.
     *
     * @param name name of field to index on
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final String name) {
        createIndex(new BasicDBObject(name, 1));
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     * @param name an identifier for the index. If null or empty, the default name will be used.
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final DBObject keys, final String name) {
        createIndex(keys, name, false);
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys   a document that contains pairs with the name of the field or fields to index and order of the index
     * @param name   an identifier for the index. If null or empty, the default name will be used.
     * @param unique if the index should be unique
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final DBObject keys, @Nullable final String name, final boolean unique) {
        DBObject options = new BasicDBObject();
        if (name != null && name.length() > 0) {
            options.put("name", name);
        }
        if (unique) {
            options.put("unique", Boolean.TRUE);
        }
        createIndex(keys, options);
    }

    /**
     * Creates an index on the field specified, if that index does not already exist.
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final DBObject keys) {
        createIndex(keys, new BasicDBObject());
    }

    /**
     * Creates an index on the field specified, if that index does not already exist.
     *
     * <p>Prior to MongoDB 3.0 the dropDups option could be used with unique indexes allowing documents with duplicate values to be dropped
     * when building the index. Later versions of MongoDB will silently ignore this setting. </p>
     *
     * @param keys    a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options a document that controls the creation of the index.
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final DBObject keys, final DBObject options) {
        try {
            executor.execute(createIndexOperation(keys, options), getReadConcern());
        } catch (MongoWriteConcernException e) {
            throw createWriteConcernException(e);
        }
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query  specifies the selection criteria for the modification
     * @param sort   determines which document the operation will modify if the query selects multiple documents
     * @param update the modifications to apply
     * @return pre-modification document
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject sort, final DBObject update) {
        return findAndModify(query, null, sort, false, update, false, false);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query  specifies the selection criteria for the modification
     * @param update the modifications to apply
     * @return the document as it was before the modifications
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, final DBObject update) {
        return findAndModify(query, null, null, false, update, false, false);
    }

    /**
     * Atomically remove and return a single document. The returned document is the original document before removal.
     *
     * @param query specifies the selection criteria for the modification
     * @return the document as it was before the modifications
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndRemove(@Nullable final DBObject query) {
        return findAndModify(query, null, null, true, null, false, false);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query     specifies the selection criteria for the modification
     * @param fields    a subset of fields to return
     * @param sort      determines which document the operation will modify if the query selects multiple documents
     * @param remove    when {@code true}, removes the selected document
     * @param returnNew when true, returns the modified document rather than the original
     * @param update    the modifications to apply
     * @param upsert    when true, operation creates a new document if the query returns no documents
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert) {
        return findAndModify(query, fields, sort, remove, update, returnNew, upsert, 0L, MILLISECONDS);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query     specifies the selection criteria for the modification
     * @param fields    a subset of fields to return
     * @param sort      determines which document the operation will modify if the query selects multiple documents
     * @param remove    when true, removes the selected document
     * @param returnNew when true, returns the modified document rather than the original
     * @param update    the modifications to apply
     * @param upsert    when true, operation creates a new document if the query returns no documents
     * @param writeConcern the write concern to apply to this operation
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @since 2.14
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, final DBObject update, final boolean returnNew,
                                  final boolean upsert, final WriteConcern writeConcern){
        return findAndModify(query, fields, sort, remove, update, returnNew, upsert, 0L, MILLISECONDS, writeConcern);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query       specifies the selection criteria for the modification
     * @param fields      a subset of fields to return
     * @param sort        determines which document the operation will modify if the query selects multiple documents
     * @param remove      when true, removes the selected document
     * @param returnNew   when true, returns the modified document rather than the original
     * @param update      the modifications to apply
     * @param upsert      when true, operation creates a new document if the query returns no documents
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it.
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.12.0
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final long maxTime, final TimeUnit maxTimeUnit) {
        return findAndModify(query, fields, sort, remove, update, returnNew, upsert, maxTime, maxTimeUnit, getWriteConcern());

    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query       specifies the selection criteria for the modification
     * @param fields      a subset of fields to return
     * @param sort        determines which document the operation will modify if the query selects multiple documents
     * @param remove      when {@code true}, removes the selected document
     * @param returnNew   when true, returns the modified document rather than the original
     * @param update      performs an update of the selected document
     * @param upsert      when true, operation creates a new document if the query returns no documents
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit the unit that maxTime is specified in
     * @param writeConcern the write concern to apply to this operation
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final long maxTime, final TimeUnit maxTimeUnit,
                                  final WriteConcern writeConcern) {
        return findAndModify(query != null ? query : new BasicDBObject(), new DBCollectionFindAndModifyOptions()
                .projection(fields)
                .sort(sort)
                .remove(remove)
                .update(update)
                .returnNew(returnNew)
                .upsert(upsert)
                .maxTime(maxTime, maxTimeUnit)
                .writeConcern(writeConcern));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query       specifies the selection criteria for the modification
     * @param fields      a subset of fields to return
     * @param sort        determines which document the operation will modify if the query selects multiple documents
     * @param remove      when {@code true}, removes the selected document
     * @param returnNew   when true, returns the modified document rather than the original
     * @param update      performs an update of the selected document
     * @param upsert      when true, operation creates a new document if the query returns no documents
     * @param bypassDocumentValidation whether to bypass document validation.
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    @Nullable
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final boolean bypassDocumentValidation,
                                  final long maxTime, final TimeUnit maxTimeUnit) {
        return findAndModify(query, fields, sort, remove, update, returnNew, upsert, bypassDocumentValidation, maxTime, maxTimeUnit,
                getWriteConcern());
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query       specifies the selection criteria for the modification
     * @param fields      a subset of fields to return
     * @param sort        determines which document the operation will modify if the query selects multiple documents
     * @param remove      when {@code true}, removes the selected document
     * @param returnNew   when true, returns the modified document rather than the original
     * @param update      performs an update of the selected document
     * @param upsert      when true, operation creates a new document if the query returns no documents
     * @param bypassDocumentValidation whether to bypass document validation.
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit the unit that maxTime is specified in
     * @param writeConcern the write concern to apply to this operation
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final boolean bypassDocumentValidation,
                                  final long maxTime, final TimeUnit maxTimeUnit,
                                  final WriteConcern writeConcern) {
        return findAndModify(query != null ? query : new BasicDBObject(), new DBCollectionFindAndModifyOptions()
                .projection(fields)
                .sort(sort)
                .remove(remove)
                .update(update)
                .returnNew(returnNew)
                .upsert(upsert)
                .bypassDocumentValidation(bypassDocumentValidation)
                .maxTime(maxTime, maxTimeUnit)
                .writeConcern(writeConcern));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query    specifies the selection criteria for the modification
     * @param options  the options regarding the find and modify operation
     * @return the document as it was before the modifications, unless {@code oprtions.returnNew} is true, in which case it returns the
     * document after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 3.4
     */
    public DBObject findAndModify(final DBObject query, final DBCollectionFindAndModifyOptions options) {
        notNull("query", query);
        notNull("options", options);
        WriteConcern optionsWriteConcern = options.getWriteConcern();
        WriteConcern writeConcern = optionsWriteConcern != null ? optionsWriteConcern : getWriteConcern();
        WriteOperation<DBObject> operation;
        if (options.isRemove()) {
            operation = new FindAndDeleteOperation<>(getNamespace(), writeConcern, retryWrites, objectCodec)
                        .filter(wrapAllowNull(query))
                        .projection(wrapAllowNull(options.getProjection()))
                        .sort(wrapAllowNull(options.getSort()))
                        .collation(options.getCollation());
        } else {
            DBObject update = options.getUpdate();
            if (update == null) {
                throw new IllegalArgumentException("update can not be null unless it's a remove");
            }
            if (!update.keySet().isEmpty() && update.keySet().iterator().next().charAt(0) == '$') {
                operation = new FindAndUpdateOperation<>(getNamespace(), writeConcern, retryWrites,
                        objectCodec, wrap(update))
                        .filter(wrap(query))
                        .projection(wrapAllowNull(options.getProjection()))
                        .sort(wrapAllowNull(options.getSort()))
                        .returnOriginal(!options.returnNew())
                        .upsert(options.isUpsert())
                        .bypassDocumentValidation(options.getBypassDocumentValidation())
                        .collation(options.getCollation())
                        .arrayFilters(wrapAllowNull(options.getArrayFilters(), (Encoder<DBObject>) null));
            } else {
                operation = new FindAndReplaceOperation<>(getNamespace(), writeConcern, retryWrites,
                        objectCodec, wrap(update))
                        .filter(wrap(query))
                        .projection(wrapAllowNull(options.getProjection()))
                        .sort(wrapAllowNull(options.getSort()))
                        .returnOriginal(!options.returnNew())
                        .upsert(options.isUpsert())
                        .bypassDocumentValidation(options.getBypassDocumentValidation())
                        .collation(options.getCollation());
            }
        }

        try {
            return getExecutor(createTimeoutSettings(getTimeoutSettings(), options)).execute(operation, getReadConcern());
        } catch (MongoWriteConcernException e) {
            throw createWriteConcernException(e);
        }
    }

    /**
     * Returns the database this collection is a member of.
     *
     * @return this collection's database
     * @mongodb.driver.manual reference/glossary/#term-database Database
     */
    public DB getDB() {
        return database;
    }

    /**
     * Get the {@link WriteConcern} for this collection.
     *
     * @return the default write concern for this collection
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public WriteConcern getWriteConcern() {
        if (writeConcern != null) {
            return writeConcern;
        }
        return database.getWriteConcern();
    }

    /**
     * Set the {@link WriteConcern} for this collection. Will be used for writes to this collection. Overrides any setting of write concern
     * at the DB level.
     *
     * @param writeConcern WriteConcern to use
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the {@link ReadPreference}.
     *
     * @return the default read preference for this collection
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public ReadPreference getReadPreference() {
        if (readPreference != null) {
            return readPreference;
        }
        return database.getReadPreference();
    }

    /**
     * Sets the {@link ReadPreference} for this collection. Will be used as default for reads from this collection; overrides DB and
     * Connection level settings. See the documentation for {@link ReadPreference} for more information.
     *
     * @param preference ReadPreference to use
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public void setReadPreference(final ReadPreference preference) {
        this.readPreference = preference;
    }

    /**
     * Sets the read concern for this collection.
     *
     * @param readConcern the read concern to use for this collection
     * @since 3.3
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public void setReadConcern(final ReadConcern readConcern) {
        this.readConcern = readConcern;
    }

    /**
     * Get the read concern for this collection.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @since 3.3
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        if (readConcern != null) {
            return readConcern;
        }
        return database.getReadConcern();
    }

    /**
     * Drops (deletes) this collection from the database. Use with care.
     *
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/drop/ Drop Command
     */
    public void drop() {
        try {
            executor.execute(new DropCollectionOperation(getNamespace(),
                            getWriteConcern()), getReadConcern());
        } catch (MongoWriteConcernException e) {
            throw createWriteConcernException(e);
        }
    }

    /**
     * Get the decoder factory for this collection.  A null return value means that the default from MongoClientOptions is being used.
     *
     * @return the factory
     */
    public DBDecoderFactory getDBDecoderFactory() {
        return withLock(factoryAndCodecLock, () -> decoderFactory);
    }

    /**
     * Set a custom decoder factory for this collection.  Set to null to use the default from MongoClientOptions.
     *
     * @param factory the factory to set.
     */
    public void setDBDecoderFactory(@Nullable final DBDecoderFactory factory) {
        withLock(factoryAndCodecLock, () -> {
            this.decoderFactory = factory;

            //Are we are using default factory?
            // If yes then we can use CollectibleDBObjectCodec directly, otherwise it will be wrapped.
            Decoder<DBObject> decoder = (factory == null || factory == DefaultDBDecoder.FACTORY)
                    ? getDefaultDBObjectCodec()
                    : new DBDecoderAdapter(factory.create(), this, PowerOfTwoBufferPool.DEFAULT);
            this.objectCodec = new CompoundDBObjectCodec(objectCodec.getEncoder(), decoder);
        });
    }

    /**
     * Get the encoder factory for this collection.  A null return value means that the default from MongoClientOptions is being used.
     *
     * @return the factory
     */
    public DBEncoderFactory getDBEncoderFactory() {
        return withLock(factoryAndCodecLock, () -> encoderFactory);
    }

    /**
     * Set a custom encoder factory for this collection.  Set to null to use the default from MongoClientOptions.
     *
     * @param factory the factory to set.
     */
    public void setDBEncoderFactory(@Nullable final DBEncoderFactory factory) {
        withLock(factoryAndCodecLock, () -> {
            this.encoderFactory = factory;

            //Are we are using default factory?
            // If yes then we can use CollectibleDBObjectCodec directly, otherwise it will be wrapped.
            Encoder<DBObject> encoder = (factory == null || factory == DefaultDBEncoder.FACTORY)
                    ? getDefaultDBObjectCodec()
                    : new DBEncoderFactoryAdapter(encoderFactory);
            this.objectCodec = new CompoundDBObjectCodec(encoder, objectCodec.getDecoder());
        });
    }

    /**
     * Return a list of the indexes for this collection.  Each object in the list is the "info document" from MongoDB
     *
     * @return list of index documents
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public List<DBObject> getIndexInfo() {
        return new MongoIterableImpl<DBObject>(null, executor, ReadConcern.DEFAULT, primary(), retryReads,
                DBCollection.this.getTimeoutSettings()) {
            @Override
            public ReadOperationCursor<DBObject> asReadOperation() {
                return new ListIndexesOperation<>(getNamespace(), getDefaultDBObjectCodec())
                        .retryReads(retryReads);
            }

            @Override
            public OperationExecutor getExecutor() {
                return executor;
            }
        }.into(new ArrayList<>());
    }

    /**
     * Drops an index from this collection.  The DBObject index parameter must match the specification of the index to drop, i.e. correct
     * key name and type must be specified.
     *
     * @param index the specification of the index to drop
     * @throws MongoException if the index does not exist
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndex(final DBObject index) {
        try {
            executor.execute(new DropIndexOperation(getNamespace(), wrap(index),
                    getWriteConcern()), getReadConcern());
        } catch (MongoWriteConcernException e) {
            throw createWriteConcernException(e);
        }
    }

    /**
     * Drops the index with the given name from this collection.
     *
     * @param indexName name of index to drop
     * @throws MongoException if the index does not exist
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndex(final String indexName) {
        try {
            executor.execute(new DropIndexOperation(getNamespace(), indexName,
                    getWriteConcern()), getReadConcern());
        } catch (MongoWriteConcernException e) {
            throw createWriteConcernException(e);
        }
    }

    /**
     * Drop all indexes on this collection.  The default index on the _id field will not be deleted.
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndexes() {
        dropIndex("*");
    }

    /**
     * Drops the index with the given name from this collection.  This method is exactly the same as {@code dropIndex(indexName)}.
     *
     * @param indexName name of index to drop
     * @throws MongoException if the index does not exist
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndexes(final String indexName) {
        dropIndex(indexName);
    }

    /**
     * Gets the default class for objects in the collection
     *
     * @return the class
     */
    public Class getObjectClass() {
        return objectFactory.getClassForPath(Collections.emptyList());
    }

    /**
     * Sets a default class for objects in this collection; null resets the class to nothing.
     *
     * @param aClass the class
     */
    public void setObjectClass(final Class<? extends DBObject> aClass) {
        setObjectFactory(objectFactory.update(aClass));
    }

    /**
     * Sets the internal class for the given path in the document hierarchy
     *
     * @param path   the path to map the given Class to
     * @param aClass the Class to map the given path to
     */
    public void setInternalClass(final String path, final Class<? extends DBObject> aClass) {
        setObjectFactory(objectFactory.update(aClass, asList(path.split("\\."))));
    }

    @Override
    public String toString() {
        return "DBCollection{database=" + database + ", name='" + name + '\'' + '}';
    }

    DBObjectFactory getObjectFactory() {
        return withLock(factoryAndCodecLock, () -> objectFactory);
    }

    void setObjectFactory(final DBCollectionObjectFactory factory) {
        withLock(factoryAndCodecLock, () -> {
            this.objectFactory = factory;
            this.objectCodec = new CompoundDBObjectCodec(objectCodec.getEncoder(), getDefaultDBObjectCodec());
        });
    }

    /**
     * <p>Creates a builder for an ordered bulk write operation, consisting of an ordered collection of write requests, which can be any
     * combination of inserts, updates, replaces, or removes. Write requests included in the bulk operations will be executed in order, and
     * will halt on the first failure.</p>
     *
     * <p>Note: While this bulk write operation will execute on MongoDB 2.4 servers and below, the writes will be performed one at a time,
     * as that is the only way to preserve the semantics of the value returned from execution or the exception thrown.</p>
     *
     * <p>Note: While a bulk write operation with a mix of inserts, updates, replaces, and removes is supported, the implementation will
     * batch up consecutive requests of the same type and send them to the server one at a time.  For example, if a bulk write operation
     * consists of 10 inserts followed by 5 updates, followed by 10 more inserts, it will result in three round trips to the server.</p>
     *
     * @return the builder
     * @mongodb.driver.manual reference/method/db.collection.initializeOrderedBulkOp/ initializeOrderedBulkOp()
     * @since 2.12
     */
    public BulkWriteOperation initializeOrderedBulkOperation() {
        return new BulkWriteOperation(true, this);
    }

    /**
     * <p>Creates a builder for an unordered bulk operation, consisting of an unordered collection of write requests, which can be any
     * combination of inserts, updates, replaces, or removes. Write requests included in the bulk operation will be executed in an undefined
     * order, and all requests will be executed even if some fail.</p>
     *
     * <p>Note: While this bulk write operation will execute on MongoDB 2.4 servers and below, the writes will be performed one at a time,
     * as that is the only way to preserve the semantics of the value returned from execution or the exception thrown.</p>
     *
     * @return the builder
     * @since 2.12
     * @mongodb.driver.manual reference/method/db.collection.initializeUnorderedBulkOp/ initializeUnorderedBulkOp()
     */
    public BulkWriteOperation initializeUnorderedBulkOperation() {
        return new BulkWriteOperation(false, this);
    }

    BulkWriteResult executeBulkWriteOperation(final boolean ordered, final Boolean bypassDocumentValidation,
                                              final List<WriteRequest> writeRequests) {
        return executeBulkWriteOperation(ordered, bypassDocumentValidation, writeRequests, getWriteConcern());
    }

    BulkWriteResult executeBulkWriteOperation(final boolean ordered, final Boolean bypassDocumentValidation,
                                              final List<WriteRequest> writeRequests,
                                              final WriteConcern writeConcern) {
        try {
            return translateBulkWriteResult(executor.execute(new MixedBulkWriteOperation(
                    getNamespace(), translateWriteRequestsToNew(writeRequests), ordered, writeConcern, false)
                    .bypassDocumentValidation(bypassDocumentValidation), getReadConcern()), getObjectCodec());
        } catch (MongoBulkWriteException e) {
            throw BulkWriteHelper.translateBulkWriteException(e, MongoClient.getDefaultCodecRegistry().get(DBObject.class));
        }
    }

    private List<com.mongodb.internal.bulk.WriteRequest> translateWriteRequestsToNew(final List<WriteRequest> writeRequests) {
        List<com.mongodb.internal.bulk.WriteRequest> retVal = new ArrayList<>(writeRequests.size());
        for (WriteRequest cur : writeRequests) {
            retVal.add(cur.toNew(this));
        }
        return retVal;
    }

    Codec<DBObject> getDefaultDBObjectCodec() {
        return new DBObjectCodec(getDB().getMongoClient().getCodecRegistry(),
                                 DBObjectCodec.getDefaultBsonTypeClassMap(),
                                 getObjectFactory())
                .withUuidRepresentation(getDB().getMongoClient().getMongoClientOptions().getUuidRepresentation());
    }

    private <T> T convertOptionsToType(final DBObject options, final String field, final Class<T> clazz) {
        return convertToType(clazz, options.get(field), format("'%s' should be of class %s", field, clazz.getSimpleName()));
    }

    @SuppressWarnings("unchecked")
    private <T> T convertToType(final Class<T> clazz, final Object value, final String errorMessage) {
        Object transformedValue = value;
        if (clazz == Boolean.class) {
            if (value instanceof Boolean) {
                transformedValue = value;
            } else if (value instanceof Number) {
                transformedValue = ((Number) value).doubleValue() != 0;
            }
        } else if (clazz == Double.class) {
            if (value instanceof Number) {
                transformedValue = ((Number) value).doubleValue();
            }
        } else if (clazz == Integer.class) {
            if (value instanceof Number) {
                transformedValue = ((Number) value).intValue();
            }
        } else if (clazz == Long.class) {
            if (value instanceof Number) {
                transformedValue = ((Number) value).longValue();
            }
        }

        if (!clazz.isAssignableFrom(transformedValue.getClass())) {
            throw new IllegalArgumentException(errorMessage);
        }
        return (T) transformedValue;
    }


    private CreateIndexesOperation createIndexOperation(final DBObject key, final DBObject options) {
        IndexRequest request = new IndexRequest(wrap(key));
        if (options.containsField("name")) {
            request.name(convertOptionsToType(options, "name", String.class));
        }
        if (options.containsField("background")) {
            request.background(convertOptionsToType(options, "background", Boolean.class));
        }
        if (options.containsField("unique")) {
            request.unique(convertOptionsToType(options, "unique", Boolean.class));
        }
        if (options.containsField("sparse")) {
            request.sparse(convertOptionsToType(options, "sparse", Boolean.class));
        }
        if (options.containsField("expireAfterSeconds")) {
            request.expireAfter(convertOptionsToType(options, "expireAfterSeconds", Long.class), TimeUnit.SECONDS);
        }
        if (options.containsField("v")) {
            request.version(convertOptionsToType(options, "v", Integer.class));
        }
        if (options.containsField("weights")) {
            request.weights(wrap(convertOptionsToType(options, "weights", DBObject.class)));
        }
        if (options.containsField("default_language")) {
            request.defaultLanguage(convertOptionsToType(options, "default_language", String.class));
        }
        if (options.containsField("language_override")) {
            request.languageOverride(convertOptionsToType(options, "language_override", String.class));
        }
        if (options.containsField("textIndexVersion")) {
            request.textVersion(convertOptionsToType(options, "textIndexVersion", Integer.class));
        }
        if (options.containsField("2dsphereIndexVersion")) {
            request.sphereVersion(convertOptionsToType(options, "2dsphereIndexVersion", Integer.class));
        }
        if (options.containsField("bits")) {
            request.bits(convertOptionsToType(options, "bits", Integer.class));
        }
        if (options.containsField("min")) {
            request.min(convertOptionsToType(options, "min", Double.class));
        }
        if (options.containsField("max")) {
            request.max(convertOptionsToType(options, "max", Double.class));
        }
        if (options.containsField("dropDups")) {
            request.dropDups(convertOptionsToType(options, "dropDups", Boolean.class));
        }
        if (options.containsField("storageEngine")) {
            request.storageEngine(wrap(convertOptionsToType(options, "storageEngine", DBObject.class)));
        }
        if (options.containsField("partialFilterExpression")) {
            request.partialFilterExpression(wrap(convertOptionsToType(options, "partialFilterExpression", DBObject.class)));
        }
        if (options.containsField("collation")) {
            request.collation(DBObjectCollationHelper.createCollationFromOptions(options));
        }
        return new CreateIndexesOperation(getNamespace(), singletonList(request), writeConcern);
    }

    Codec<DBObject> getObjectCodec() {
        return objectCodec;
    }

    OperationExecutor getExecutor() {
        return executor;
    }

    MongoNamespace getNamespace() {
        return new MongoNamespace(getDB().getName(), getName());
    }

    @Nullable
    BsonDocument wrapAllowNull(@Nullable final DBObject document) {
        if (document == null) {
            return null;
        }
        return wrap(document);
    }

    @Nullable
    List<BsonDocument> wrapAllowNull(@Nullable final List<? extends DBObject> documentList, @Nullable final DBEncoder encoder) {
        return wrapAllowNull(documentList, encoder == null ? null : new DBEncoderAdapter(encoder));
    }

    @Nullable
    List<BsonDocument> wrapAllowNull(@Nullable final List<? extends DBObject> documentList, @Nullable final Encoder<DBObject> encoder) {
        if (documentList == null) {
            return null;
        }
        List<BsonDocument> wrappedDocumentList = new ArrayList<>(documentList.size());
        for (DBObject cur : documentList) {
            wrappedDocumentList.add(encoder == null ? wrap(cur) : wrap(cur, encoder));
        }
        return wrappedDocumentList;
    }


    BsonDocument wrap(final DBObject document) {
        return new BsonDocumentWrapper<>(document, getDefaultDBObjectCodec());
    }

    BsonDocument wrap(final DBObject document, @Nullable final DBEncoder encoder) {
        if (encoder == null) {
            return wrap(document);
        } else {
            return new BsonDocumentWrapper<>(document, new DBEncoderAdapter(encoder));
        }
    }

    BsonDocument wrap(final DBObject document, @Nullable final Encoder<DBObject> encoder) {
        if (encoder == null) {
            return wrap(document);
        } else {
            return new BsonDocumentWrapper<>(document, encoder);
        }
    }

    TimeoutSettings getTimeoutSettings(){
       return database.getTimeoutSettings();
    }

    static WriteConcernException createWriteConcernException(final MongoWriteConcernException e) {
        return new WriteConcernException(new BsonDocument("code", new BsonInt32(e.getWriteConcernError().getCode()))
                                                .append("errmsg", new BsonString(e.getWriteConcernError().getMessage())),
                                               e.getServerAddress(),
                                               e.getWriteResult());
    }

    private OperationExecutor getExecutor(final TimeoutSettings timeoutSettings) {
        return executor.withTimeoutSettings(timeoutSettings);
    }

}
