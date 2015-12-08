/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.IndexRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.connection.BufferProvider;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.BaseWriteOperation;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateIndexesOperation;
import com.mongodb.operation.DeleteOperation;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.FindAndDeleteOperation;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.MapReduceBatchCursor;
import com.mongodb.operation.MapReduceStatistics;
import com.mongodb.operation.MapReduceToCollectionOperation;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.operation.MixedBulkWriteOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ParallelCollectionScanOperation;
import com.mongodb.operation.RenameCollectionOperation;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.WriteOperation;
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
import java.util.concurrent.TimeUnit;

import static com.mongodb.AggregationOptions.OutputMode.CURSOR;
import static com.mongodb.AggregationOptions.OutputMode.INLINE;
import static com.mongodb.BulkWriteHelper.translateBulkWriteResult;
import static com.mongodb.BulkWriteHelper.translateWriteRequestsToNew;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.primaryPreferred;
import static java.lang.String.format;
import static java.util.Arrays.asList;
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
 * See {@link Mongo#getDB(String)} for further information about the effective deprecation of this class.
 *
 * @mongodb.driver.manual reference/glossary/#term-collection Collection
 */
@ThreadSafe
@SuppressWarnings({"rawtypes", "deprecation"})
public class DBCollection {
    public static final String ID_FIELD_NAME = "_id";
    private final String name;
    private final DB database;
    private final OperationExecutor executor;
    private final Bytes.OptionHolder optionHolder;
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;
    private volatile ReadConcern readConcern;
    private List<DBObject> hintFields;
    private DBEncoderFactory encoderFactory;
    private DBDecoderFactory decoderFactory;
    private DBCollectionObjectFactory objectFactory;
    private volatile CompoundDBObjectCodec objectCodec;


    /**
     * Constructs new {@code DBCollection} instance. This operation not reflected on the server.
     *  @param name          the name of the collection
     * @param database      the database to which this collections belongs to
     */
    DBCollection(final String name, final DB database, final OperationExecutor executor) {
        this.name = name;
        this.database = database;
        this.executor = executor;
        this.optionHolder = new Bytes.OptionHolder(database.getOptionHolder());
        this.objectFactory = new DBCollectionObjectFactory();
        this.objectCodec = new CompoundDBObjectCodec(getDefaultDBObjectCodec());
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

    private static BasicDBList toDBList(final BatchCursor<DBObject> source) {
        BasicDBList dbList = new BasicDBList();
        while (source.hasNext()) {
            dbList.addAll(source.next());
        }
        return dbList;
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
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws MongoException if the operation failed for some other reason
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents, final WriteConcern aWriteConcern, final DBEncoder dbEncoder) {
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents, final InsertOptions insertOptions) {
        WriteConcern writeConcern = insertOptions.getWriteConcern() != null ? insertOptions.getWriteConcern() : getWriteConcern();
        Encoder<DBObject> encoder = toEncoder(insertOptions.getDbEncoder());

        List<InsertRequest> insertRequestList = new ArrayList<InsertRequest>(documents.size());
        for (DBObject cur : documents) {
            if (cur.get(ID_FIELD_NAME) == null) {
                cur.put(ID_FIELD_NAME, new ObjectId());
            }
            insertRequestList.add(new InsertRequest(new BsonDocumentWrapper<DBObject>(cur, encoder)));
        }
        return insert(insertRequestList, writeConcern, insertOptions.isContinueOnError(), insertOptions.getBypassDocumentValidation());
    }

    private Encoder<DBObject> toEncoder(final DBEncoder dbEncoder) {
        return dbEncoder != null ? new DBEncoderAdapter(dbEncoder) : objectCodec;
    }

    private WriteResult insert(final List<InsertRequest> insertRequestList, final WriteConcern writeConcern,
                               final boolean continueOnError, final Boolean bypassDocumentValidation) {
        return executeWriteOperation(new InsertOperation(getNamespace(), !continueOnError, writeConcern, insertRequestList)
                                     .bypassDocumentValidation(bypassDocumentValidation));
    }

    WriteResult executeWriteOperation(final BaseWriteOperation operation) {
        return translateWriteResult(executor.execute(operation));
    }

    private WriteResult translateWriteResult(final WriteConcernResult writeConcernResult) {
        if (!writeConcernResult.wasAcknowledged()) {
            return WriteResult.unacknowledged();
        }

        return translateWriteResult(writeConcernResult.getCount(), writeConcernResult.isUpdateOfExisting(),
                                    writeConcernResult.getUpsertedId());
    }

    private WriteResult translateWriteResult(final int count, final boolean isUpdateOfExisting, final BsonValue upsertedId) {
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

    @SuppressWarnings("unchecked")
    private WriteResult replaceOrInsert(final DBObject obj, final Object id, final WriteConcern writeConcern) {
        DBObject filter = new BasicDBObject(ID_FIELD_NAME, id);

        UpdateRequest replaceRequest = new UpdateRequest(wrap(filter), wrap(obj, objectCodec),
                                                         com.mongodb.bulk.WriteRequest.Type.REPLACE).upsert(true);

        return executeWriteOperation(new UpdateOperation(getNamespace(), false, writeConcern, asList(replaceRequest)));
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    @SuppressWarnings("unchecked")
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern concern, final DBEncoder encoder) {
        return updateImpl(query, update, upsert, multi, concern, null, encoder);
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     * @since 2.14
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern concern, final boolean bypassDocumentValidation, final DBEncoder encoder) {
        return updateImpl(query, update, upsert, multi, concern, bypassDocumentValidation, encoder);
    }

    private WriteResult updateImpl(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                                   final WriteConcern concern, final Boolean bypassDocumentValidation, final DBEncoder encoder) {
        if (update == null) {
            throw new IllegalArgumentException("update can not be null");
        }

        if (query == null) {
            throw new IllegalArgumentException("update query can not be null");
        }

        if (!update.keySet().isEmpty() && update.keySet().iterator().next().startsWith("$")) {
            UpdateRequest updateRequest = new UpdateRequest(wrap(query), wrap(update, encoder),
                                                            com.mongodb.bulk.WriteRequest.Type.UPDATE).upsert(upsert).multi(multi);

            return executeWriteOperation(new UpdateOperation(getNamespace(), false, concern, asList(updateRequest))
                                         .bypassDocumentValidation(bypassDocumentValidation));
        } else {
            UpdateRequest replaceRequest = new UpdateRequest(wrap(query), wrap(update, encoder),
                                                             com.mongodb.bulk.WriteRequest.Type.REPLACE)
                                           .upsert(upsert);
            return executeWriteOperation(new UpdateOperation(getNamespace(), true, concern, asList(replaceRequest))
                                         .bypassDocumentValidation(bypassDocumentValidation));
        }
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult updateMulti(final DBObject query, final DBObject update) {
        return update(query, update, false, true);
    }

    /**
     * Remove documents from a collection.
     *
     * @param query the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents
     *              in the collection.
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     */
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern) {
        return executeWriteOperation(new DeleteOperation(getNamespace(), false, writeConcern, asList(new DeleteRequest(wrap(query)))));
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     */
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern, final DBEncoder encoder) {
        DeleteRequest deleteRequest = new DeleteRequest(wrap(query, encoder));

        return executeWriteOperation(new DeleteOperation(getNamespace(), false, writeConcern, asList(deleteRequest)));
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query      the selection criteria using query operators. Omit the query parameter or pass an empty document to return all
     *                   documents in the collection.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @param numToSkip  number of documents to skip
     * @param batchSize  see {@link DBCursor#batchSize(int)} for more information
     * @param options    query options to be used
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     * @deprecated use {@link com.mongodb.DBCursor#skip(int)}, {@link com.mongodb.DBCursor#batchSize(int)} and {@link
     * com.mongodb.DBCursor#setOptions(int)} on the {@code DBCursor} returned from {@link com.mongodb.DBCollection#find(DBObject,
     * DBObject)}
     */
    @Deprecated
    public DBCursor find(final DBObject query, final DBObject projection, final int numToSkip, final int batchSize,
                         final int options) {
        return new DBCursor(this, query, projection, getReadPreference()).batchSize(batchSize).skip(numToSkip).setOptions(options);
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query      the selection criteria using query operators. Omit the query parameter or pass an empty document to return all
     *                   documents in the collection.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @param numToSkip  number of documents to skip
     * @param batchSize  see {@link DBCursor#batchSize(int)} for more information
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     * @deprecated use {@link com.mongodb.DBCursor#skip(int)} and {@link com.mongodb.DBCursor#batchSize(int)} on the {@code DBCursor}
     * returned from {@link com.mongodb.DBCollection#find(DBObject, DBObject)}
     */
    @Deprecated
    public DBCursor find(final DBObject query, final DBObject projection, final int numToSkip, final int batchSize) {
        return new DBCursor(this, query, projection, getReadPreference()).batchSize(batchSize).skip(numToSkip);
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
     * Get a single document from collection.
     *
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
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
    public DBObject findOne(final DBObject query, final DBObject projection, final DBObject sort,
                            final ReadPreference readPreference) {
        return findOne(query, projection, sort, readPreference, getReadConcern(), 0, MILLISECONDS);
    }

    /**
     * Get a single document from collection.
     *
     * @param query          the selection criteria using query operators.
     * @param projection     specifies which projection MongoDB will return from the documents in the result set.
     * @param sort           A document whose fields specify the attributes on which to sort the result set.
     * @param readPreference {@code ReadPreference} to be used for this operation
     * @param readConcern    {@code ReadConcern} to be used for this operation
     * @param maxTime        the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit    the unit that maxTime is specified in
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     * @since 2.12.0
     */
    DBObject findOne(final DBObject query, final DBObject projection, final DBObject sort,
                     final ReadPreference readPreference, final ReadConcern readConcern,
                     final long maxTime, final TimeUnit maxTimeUnit) {
        FindOperation<DBObject> operation = new FindOperation<DBObject>(getNamespace(),
                                                                            objectCodec)
                                                    .readConcern(readConcern)
                                                    .projection(wrapAllowNull(projection))
                                                    .sort(wrapAllowNull(sort))
                                                    .limit(-1)
                                                    .maxTime(maxTime, maxTimeUnit);
        if (query != null) {
            operation.filter(wrap(query));
        }
        BatchCursor<DBObject> cursor = executor.execute(operation, readPreference);
        return cursor.hasNext() ? cursor.next().iterator().next() : null;
    }

    /**
     * Get a single document from collection by '_id'.
     *
     * @param id value of '_id' field of a document we are looking for
     * @return A document with '_id' provided as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    public DBObject findOne(final Object id) {
        return findOne(id, null);
    }

    /**
     * Get a single document from collection by '_id'.
     *
     * @param id         value of '_id' field of a document we are looking for
     * @param projection specifies which projection MongoDB will return from the documents in the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    public DBObject findOne(final Object id, final DBObject projection) {
        return findOne(new BasicDBObject("_id", id), projection);
    }

    /**
     * Same as {@link #getCount()}
     *
     * @return the number of documents in collection
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count() {
        return getCount(new BasicDBObject(), null);
    }

    /**
     * Same as {@link #getCount(DBObject)}
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count(final DBObject query) {
        return getCount(query, null);
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
    public long count(final DBObject query, final ReadPreference readPreference) {
        return getCount(query, null, readPreference);
    }

    /**
     * Get the count of documents in collection.
     *
     * @return the number of documents in collection
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount() {
        return getCount(new BasicDBObject(), null);
    }

    /**
     * Get the count of documents in collection.
     *
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the number of documents in collection
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(final ReadPreference readPreference) {
        return getCount(new BasicDBObject(), null, readPreference);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(final DBObject query) {
        return getCount(query, null);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query      specifies the selection criteria
     * @param projection this is ignored
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(final DBObject query, final DBObject projection) {
        return getCount(query, projection, 0, 0);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query          specifies the selection criteria
     * @param projection     this is ignored
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(final DBObject query, final DBObject projection, final ReadPreference readPreference) {
        return getCount(query, projection, 0, 0, readPreference);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query      specifies the selection criteria
     * @param projection this is ignored
     * @param limit      limit the count to this value
     * @param skip       number of documents to skip
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(final DBObject query, final DBObject projection, final long limit, final long skip) {
        return getCount(query, projection, limit, skip, getReadPreference());
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query          specifies the selection criteria
     * @param projection     this is ignored
     * @param limit          limit the count to this value
     * @param skip           number of documents to skip
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(final DBObject query, final DBObject projection, final long limit, final long skip,
                         final ReadPreference readPreference) {
        return getCount(query, limit, skip, readPreference, getReadConcern(), 0, MILLISECONDS);
    }

    long getCount(final DBObject query, final long limit, final long skip,
                  final ReadPreference readPreference, final ReadConcern readConcern,
                  final long maxTime, final TimeUnit maxTimeUnit) {
        return getCount(query, limit, skip, readPreference, readConcern, maxTime, maxTimeUnit, null);
    }

    long getCount(final DBObject query, final long limit, final long skip,
                  final ReadPreference readPreference, final ReadConcern readConcern,
                  final long maxTime, final TimeUnit maxTimeUnit,
                  final BsonValue hint) {

        if (limit > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("limit is too large: " + limit);
        }

        if (skip > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("skip is too large: " + skip);
        }

        CountOperation operation = new CountOperation(getNamespace())
                                       .readConcern(readConcern)
                                       .hint(hint)
                                       .skip(skip)
                                       .limit(limit)
                                       .maxTime(maxTime, maxTimeUnit);
        if (query != null) {
            operation.filter(wrap(query));
        }
        return executor.execute(operation, readPreference);
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
        executor.execute(new RenameCollectionOperation(getNamespace(),
                                                       new MongoNamespace(getNamespace().getDatabaseName(),
                                                                          newName)).dropTarget(dropTarget));
        return getDB().getCollection(newName);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param key     specifies one or more document fields to group
     * @param cond    specifies the selection criteria to determine which documents in the collection to process
     * @param initial initializes the aggregation result document
     * @param reduce  specifies an $reduce function, that operates on the documents during the grouping operation
     * @return a document with the grouped records as well as the command meta-data
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group(final DBObject key, final DBObject cond, final DBObject initial, final String reduce) {
        return group(key, cond, initial, reduce, null);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param key      specifies one or more document fields to group
     * @param cond     specifies the selection criteria to determine which documents in the collection to process
     * @param initial  initializes the aggregation result document
     * @param reduce   specifies an $reduce Javascript function, that operates on the documents during the grouping operation
     * @param finalize specifies a Javascript function that runs each item in the result set before final value will be returned
     * @return a document with the grouped records as well as the command meta-data
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group(final DBObject key, final DBObject cond, final DBObject initial, final String reduce,
                          final String finalize) {
        return group(key, cond, initial, reduce, finalize, getReadPreference());
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param key            specifies one or more document fields to group
     * @param cond           specifies the selection criteria to determine which documents in the collection to process
     * @param initial        initializes the aggregation result document
     * @param reduce         specifies an $reduce Javascript function, that operates on the documents during the grouping operation
     * @param finalize       specifies a Javascript function that runs each item in the result set before final value will be returned
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return a document with the grouped records as well as the command meta-data
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group(final DBObject key, final DBObject cond, final DBObject initial, final String reduce,
                          final String finalize, final ReadPreference readPreference) {
        return group(new GroupCommand(this, key, cond, initial, reduce, finalize), readPreference);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param cmd the group command
     * @return a document with the grouped records as well as the command meta-data
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group(final GroupCommand cmd) {
        return group(cmd, getReadPreference());
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param cmd            the group command
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return a document with the grouped records as well as the command meta-data
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group(final GroupCommand cmd, final ReadPreference readPreference) {
        return toDBList(executor.execute(cmd.toOperation(getNamespace(), getDefaultDBObjectCodec()), readPreference));
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
    @SuppressWarnings("unchecked")
    public List distinct(final String fieldName, final DBObject query, final ReadPreference readPreference) {
        return new OperationIterable<BsonValue>(new DistinctOperation<BsonValue>(getNamespace(), fieldName,
                                                new BsonValueCodec())
                                                .readConcern(getReadConcern())
                                                .filter(wrap(query)),
                                                readPreference, executor).map(new Function<BsonValue, Object>() {
            @Override
            public Object apply(final BsonValue bsonValue) {
                BsonDocument document = new BsonDocument("value", bsonValue);
                DBObject obj = getDefaultDBObjectCodec().decode(new BsonDocumentReader(document), DecoderContext.builder().build());
                return obj.get("value");
            }
        }).into(new ArrayList<Object>());
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
        if (command.getOutputType() == MapReduceCommand.OutputType.INLINE) {

            MapReduceWithInlineResultsOperation<DBObject> operation =
                new MapReduceWithInlineResultsOperation<DBObject>(getNamespace(),
                                                                  new BsonJavaScript(command.getMap()),
                                                                  new BsonJavaScript(command.getReduce()),
                                                                  getDefaultDBObjectCodec());
            operation.readConcern(getReadConcern());
            operation.filter(wrapAllowNull(command.getQuery()));
            operation.limit(command.getLimit());
            operation.maxTime(command.getMaxTime(MILLISECONDS), MILLISECONDS);
            operation.jsMode(command.getJsMode() == null ? false : command.getJsMode());
            operation.sort(wrapAllowNull(command.getSort()));
            operation.verbose(command.isVerbose());

            if (command.getScope() != null) {
                operation.scope(wrap(new BasicDBObject(command.getScope())));
            }
            if (command.getFinalize() != null) {
                operation.finalizeFunction(new BsonJavaScript(command.getFinalize()));
            }
            MapReduceBatchCursor<DBObject> executionResult = executor.execute(operation, readPreference);
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
                new MapReduceToCollectionOperation(getNamespace(),
                                                   new BsonJavaScript(command.getMap()),
                                                   new BsonJavaScript(command.getReduce()),
                                                   command.getOutputTarget())
                    .filter(wrapAllowNull(command.getQuery()))
                    .limit(command.getLimit())
                    .maxTime(command.getMaxTime(MILLISECONDS), MILLISECONDS)
                    .jsMode(command.getJsMode() == null ? false : command.getJsMode())
                    .sort(wrapAllowNull(command.getSort()))
                    .verbose(command.isVerbose())
                    .action(action)
                    .databaseName(command.getOutputDB())
                    .bypassDocumentValidation(command.getBypassDocumentValidation());

            if (command.getScope() != null) {
                operation.scope(wrap(new BasicDBObject(command.getScope())));
            }
            if (command.getFinalize() != null) {
                operation.finalizeFunction(new BsonJavaScript(command.getFinalize()));
            }
            MapReduceStatistics mapReduceStatistics = executor.execute(operation);
            DBCollection mapReduceOutputCollection = getMapReduceOutputCollection(command);
            DBCursor executionResult = mapReduceOutputCollection.find();
            return new MapReduceOutput(command.toDBObject(), executionResult, mapReduceStatistics, mapReduceOutputCollection);
        }
    }

    private DBCollection getMapReduceOutputCollection(final MapReduceCommand command) {
        String requestedDatabaseName = command.getOutputDB();
        DB database = requestedDatabaseName != null
                      ? getDB().getSisterDB(requestedDatabaseName)
                      : getDB();
        return database.getCollection(command.getOutputTarget());
    }

    /**
     * Method implements aggregation framework.
     *
     * @param firstOp       requisite first operation to be performed in the aggregation pipeline
     * @param additionalOps additional operations to be performed in the aggregation pipeline
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     * @deprecated Use {@link com.mongodb.DBCollection#aggregate(java.util.List)} instead
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public AggregationOutput aggregate(final DBObject firstOp, final DBObject... additionalOps) {
        List<DBObject> pipeline = new ArrayList<DBObject>();
        pipeline.add(firstOp);
        Collections.addAll(pipeline, additionalOps);
        return aggregate(pipeline);
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline operations to be performed in the aggregation pipeline
     * @return the aggregation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    public AggregationOutput aggregate(final List<? extends DBObject> pipeline) {
        return aggregate(pipeline, getReadPreference());
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline       operations to be performed in the aggregation pipeline
     * @param readPreference the read preference specifying where to run the query
     * @return the aggregation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    @SuppressWarnings("unchecked")
    public AggregationOutput aggregate(final List<? extends DBObject> pipeline, final ReadPreference readPreference) {
        Cursor cursor = aggregate(pipeline, AggregationOptions.builder().outputMode(INLINE).build(), readPreference, false);

        if (cursor == null) {
            return new AggregationOutput(Collections.<DBObject>emptyList());
        } else {
            List<DBObject> results = new ArrayList<DBObject>();
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
            return new AggregationOutput(results);

        }
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
        return aggregate(pipeline, options, readPreference, true);
    }

    private Cursor aggregate(final List<? extends DBObject> pipeline, final AggregationOptions options, final ReadPreference readPreference,
                             final boolean returnCursorForOutCollection) {
        if (options == null) {
            throw new IllegalArgumentException("options can not be null");
        }
        List<BsonDocument> stages = preparePipeline(pipeline);

        BsonValue outCollection = stages.get(stages.size() - 1).get("$out");

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(getNamespace(), stages)
                                                       .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                       .allowDiskUse(options.getAllowDiskUse())
                                                       .bypassDocumentValidation(options.getBypassDocumentValidation());
            executor.execute(operation);
            if (returnCursorForOutCollection) {
                return new DBCursor(database.getCollection(outCollection.asString().getValue()), new BasicDBObject(), null, primary());
            } else {
                return null;
            }
        } else {
            AggregateOperation<DBObject> operation = new AggregateOperation<DBObject>(getNamespace(), stages, getDefaultDBObjectCodec())
                                                         .readConcern(getReadConcern())
                                                         .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                         .allowDiskUse(options.getAllowDiskUse())
                                                         .batchSize(options.getBatchSize())
                                                         .useCursor(options.getOutputMode() == CURSOR);
            BatchCursor<DBObject> cursor = executor.execute(operation, readPreference);
            return new MongoCursorAdapter(new MongoBatchCursorAdapter<DBObject>(cursor));
        }
    }

    /**
     * Return the explain plan for the aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline to explain
     * @param options  the options to apply to the aggregation
     * @return the command result.  The explain output may change from release to release, so best to simply log this.
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.driver.manual reference/operator/meta/explain/ Explain query
     * @mongodb.server.release 2.6
     */
    public CommandResult explainAggregate(final List<? extends DBObject> pipeline, final AggregationOptions options) {
        AggregateOperation<BsonDocument> operation = new AggregateOperation<BsonDocument>(getNamespace(), preparePipeline(pipeline),
                                                                                          new BsonDocumentCodec())
                                                         .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                         .allowDiskUse(options.getAllowDiskUse());
        return new CommandResult(executor.execute(operation.asExplainableOperation(ExplainVerbosity.QUERY_PLANNER), primaryPreferred()));
    }

    @SuppressWarnings("unchecked")
    private List<BsonDocument> preparePipeline(final List<? extends DBObject> pipeline) {
        if (pipeline.isEmpty()) {
            throw new MongoException("Aggregation pipelines can not be empty");
        }
        List<BsonDocument> stages = new ArrayList<BsonDocument>();
        for (final DBObject op : pipeline) {
            stages.add(wrap(op));
        }

        return stages;
    }

    /**
     * <p>Return a list of cursors over the collection that can be used to scan it in parallel.</p>
     *
     * <p>Note: As of MongoDB 2.6, this method will work against a mongod, but not a mongos. </p>
     *
     * @param options the parallel scan options
     * @return a list of cursors, whose size may be less than the number requested
     * @mongodb.driver.manual reference/command/parallelCollectionScan/ Parallel Collection Scan
     * @mongodb.server.release 2.6
     * @since 2.12
     */
    public List<Cursor> parallelScan(final ParallelScanOptions options) {
        List<Cursor> cursors = new ArrayList<Cursor>();
        ParallelCollectionScanOperation<DBObject> operation = new ParallelCollectionScanOperation<DBObject>(getNamespace(),
                                                                                                            options.getNumCursors(),
                                                                                                            objectCodec)
                                                                  .readConcern(getReadConcern())
                                                                  .batchSize(options.getBatchSize());
        List<BatchCursor<DBObject>> mongoCursors = executor.execute(operation,
                                                                    options.getReadPreference() != null ? options.getReadPreference()
                                                                                                        : getReadPreference());

        for (BatchCursor<DBObject> mongoCursor : mongoCursors) {
            cursors.add(new MongoCursorAdapter(new MongoBatchCursorAdapter<DBObject>(mongoCursor)));
        }
        return cursors;
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
    public void createIndex(final DBObject keys, final String name, final boolean unique) {
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
        executor.execute(createIndexOperation(keys, options));
    }

    /**
     * Get hint fields for this collection (used to optimize queries).
     *
     * @return a list of {@code DBObject} to be used as hints.
     * @mongodb.driver.manual reference/operator/meta/hint/ $hint
     */
    public List<DBObject> getHintFields() {
        return hintFields;
    }

    /**
     * Override MongoDB's default index selection and query optimization process.
     *
     * @param indexes list of indexes to "hint" or force MongoDB to use when performing the query.
     * @mongodb.driver.manual reference/operator/meta/hint/ $hint
     */
    public void setHintFields(final List<? extends DBObject> indexes) {
        this.hintFields = new ArrayList<DBObject>(indexes);
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndModify(final DBObject query, final DBObject sort, final DBObject update) {
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndModify(final DBObject query, final DBObject update) {
        return findAndModify(query, null, null, false, update, false, false);
    }

    /**
     * Atomically remove and return a single document. The returned document is the original document before removal.
     *
     * @param query specifies the selection criteria for the modification
     * @return the document as it was before the modifications
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndRemove(final DBObject query) {
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
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, final DBObject update,
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
     * @throws MongoException if the operation failed for some other reason
     * @since 2.14
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort, final boolean remove,
                                  final DBObject update, final boolean returnNew,
                                  final boolean upsert, final WriteConcern writeConcern){
        return findAndModifyImpl(query, fields, sort, remove, update, returnNew, upsert, null, 0L, MILLISECONDS, writeConcern);
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
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it. A non-zero value requires
     *                    a server version &gt;= 2.6
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.12.0
     */
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final long maxTime, final TimeUnit maxTimeUnit) {
        return findAndModifyImpl(query, fields, sort, remove, update, returnNew, upsert, null, maxTime, maxTimeUnit, getWriteConcern());

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
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it. A non-zero value requires
     *                    a server version &gt;= 2.6
     * @param maxTimeUnit the unit that maxTime is specified in
     * @param writeConcern the write concern to apply to this operation
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final long maxTime, final TimeUnit maxTimeUnit,
                                  final WriteConcern writeConcern) {
        return findAndModifyImpl(query, fields, sort, remove, update, returnNew, upsert, null, maxTime, maxTimeUnit, writeConcern);
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
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it. A non-zero value requires
     *                    a server version &gt;= 2.6
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final boolean bypassDocumentValidation,
                                  final long maxTime, final TimeUnit maxTimeUnit) {
        return findAndModifyImpl(query, fields, sort, remove, update, returnNew, upsert, bypassDocumentValidation, maxTime, maxTimeUnit,
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
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it. A non-zero value requires
     *                    a server version &gt;= 2.6
     * @param maxTimeUnit the unit that maxTime is specified in
     * @param writeConcern the write concern to apply to this operation
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException if the write failed due some other failure specific to the update command
     * @throws MongoException if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final boolean bypassDocumentValidation,
                                  final long maxTime, final TimeUnit maxTimeUnit,
                                  final WriteConcern writeConcern) {
        return findAndModifyImpl(query, fields, sort, remove, update, returnNew, upsert, bypassDocumentValidation, maxTime, maxTimeUnit,
                                 writeConcern);
    }

    private DBObject findAndModifyImpl(final DBObject query, final DBObject fields, final DBObject sort,
                                       final boolean remove, final DBObject update,
                                       final boolean returnNew, final boolean upsert,
                                       final Boolean bypassDocumentValidation,
                                       final long maxTime, final TimeUnit maxTimeUnit,
                                       final WriteConcern writeConcern) {
        WriteOperation<DBObject> operation;
        if (remove) {
            operation = new FindAndDeleteOperation<DBObject>(getNamespace(), writeConcern, objectCodec)
                        .filter(wrapAllowNull(query))
                        .projection(wrapAllowNull(fields))
                        .sort(wrapAllowNull(sort))
                        .maxTime(maxTime, maxTimeUnit);
        } else {
            if (update == null) {
                throw new IllegalArgumentException("Update document can't be null");
            }
            if (!update.keySet().isEmpty() && update.keySet().iterator().next().charAt(0) == '$') {
                operation = new FindAndUpdateOperation<DBObject>(getNamespace(), writeConcern, objectCodec, wrapAllowNull(update))
                            .filter(wrap(query))
                            .projection(wrapAllowNull(fields))
                            .sort(wrapAllowNull(sort))
                            .returnOriginal(!returnNew)
                            .upsert(upsert)
                            .maxTime(maxTime, maxTimeUnit)
                            .bypassDocumentValidation(bypassDocumentValidation);
            } else {
                operation = new FindAndReplaceOperation<DBObject>(getNamespace(), writeConcern, objectCodec, wrap(update))
                            .filter(wrapAllowNull(query))
                            .projection(wrapAllowNull(fields))
                            .sort(wrapAllowNull(sort))
                            .returnOriginal(!returnNew)
                            .upsert(upsert)
                            .maxTime(maxTime, maxTimeUnit)
                            .bypassDocumentValidation(bypassDocumentValidation);
            }
        }

        try {
            return executor.execute(operation);
        } catch (MongoWriteConcernException e) {
            throw new WriteConcernException(new BsonDocument("code", new BsonInt32(e.getWriteConcernError().getCode()))
                                           .append("errmsg", new BsonString(e.getWriteConcernError().getMessage())),
                                            e.getServerAddress(),
                                            e.getWriteResult());
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
     * Makes this query ok to run on a slave node
     *
     * @deprecated Replaced with {@link ReadPreference#secondaryPreferred()}
     */
    @Deprecated
    public void slaveOk() {
        addOption(Bytes.QUERYOPTION_SLAVEOK);
    }

    /**
     * Adds the given flag to the default query options.
     *
     * @param option value to be added
     * @mongodb.driver.manual reference/method/cursor.addOption/#flags Query Flags
     */
    public void addOption(final int option) {
        optionHolder.add(option);
    }

    /**
     * Resets the default query options
     *
     * @mongodb.driver.manual reference/method/cursor.addOption/#flags Query Flags
     */
    public void resetOptions() {
        optionHolder.reset();
    }

    /**
     * Gets the default query options
     *
     * @return bit vector of query options
     * @mongodb.driver.manual reference/method/cursor.addOption/#flags Query Flags
     */
    public int getOptions() {
        return optionHolder.get();
    }

    /**
     * Sets the default query options, overwriting previous value.
     *
     * @param options bit vector of query options
     * @mongodb.driver.manual reference/method/cursor.addOption/#flags Query Flags
     */
    public void setOptions(final int options) {
        optionHolder.set(options);
    }

    /**
     * Drops (deletes) this collection from the database. Use with care.
     *
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/drop/ Drop Command
     */
    public void drop() {
        executor.execute(new DropCollectionOperation(getNamespace()));
    }

    /**
     * Get the decoder factory for this collection.  A null return value means that the default from MongoOptions is being used.
     *
     * @return the factory
     */
    public synchronized DBDecoderFactory getDBDecoderFactory() {
        return decoderFactory;
    }

    /**
     * Set a custom decoder factory for this collection.  Set to null to use the default from MongoOptions.
     *
     * @param factory the factory to set.
     */
    public synchronized void setDBDecoderFactory(final DBDecoderFactory factory) {
        this.decoderFactory = factory;

        //Are we are using default factory?
        // If yes then we can use CollectibleDBObjectCodec directly, otherwise it will be wrapped.
        Decoder<DBObject> decoder = (factory == null || factory == DefaultDBDecoder.FACTORY)
                                    ? getDefaultDBObjectCodec()
                                    : new DBDecoderAdapter(factory.create(), this, getBufferPool());
        this.objectCodec = new CompoundDBObjectCodec(objectCodec.getEncoder(), decoder);
    }

    /**
     * Get the encoder factory for this collection.  A null return value means that the default from MongoOptions is being used.
     *
     * @return the factory
     */
    public synchronized DBEncoderFactory getDBEncoderFactory() {
        return this.encoderFactory;
    }

    /**
     * Set a custom encoder factory for this collection.  Set to null to use the default from MongoOptions.
     *
     * @param factory the factory to set.
     */
    public synchronized void setDBEncoderFactory(final DBEncoderFactory factory) {
        this.encoderFactory = factory;

        //Are we are using default factory?
        // If yes then we can use CollectibleDBObjectCodec directly, otherwise it will be wrapped.
        Encoder<DBObject> encoder = (factory == null || factory == DefaultDBEncoder.FACTORY)
                                    ? getDefaultDBObjectCodec()
                                    : new DBEncoderFactoryAdapter(encoderFactory);
        this.objectCodec = new CompoundDBObjectCodec(encoder, objectCodec.getDecoder());
    }

    /**
     * Return a list of the indexes for this collection.  Each object in the list is the "info document" from MongoDB
     *
     * @return list of index documents
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public List<DBObject> getIndexInfo() {
        return new OperationIterable<DBObject>(new ListIndexesOperation<DBObject>(getNamespace(), getDefaultDBObjectCodec()),
                                               primary(), executor).into(new ArrayList<DBObject>());
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
        dropIndex(getIndexNameFromIndexFields(index));
    }

    /**
     * Drops the index with the given name from this collection.
     *
     * @param indexName name of index to drop
     * @throws MongoException if the index does not exist
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndex(final String indexName) {
        executor.execute(new DropIndexOperation(getNamespace(), indexName));
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
     * The collStats command returns a variety of storage statistics for a given collection
     *
     * @return a CommandResult containing the statistics about this collection
     * @mongodb.driver.manual reference/command/collStats/ collStats Command
     */
    public CommandResult getStats() {
        return getDB().executeCommand(new BsonDocument("collStats", new BsonString(getName())), getReadPreference());
    }

    /**
     * Checks whether this collection is capped
     *
     * @return true if this is a capped collection
     * @mongodb.driver.manual /core/capped-collections/#check-if-a-collection-is-capped Capped Collections
     */
    public boolean isCapped() {
        CommandResult commandResult = getStats();
        Object cappedField = commandResult.get("capped");
        return cappedField != null && (cappedField.equals(1) || cappedField.equals(true));
    }

    /**
     * Gets the default class for objects in the collection
     *
     * @return the class
     */
    public Class getObjectClass() {
        return objectFactory.getClassForPath(Collections.<String>emptyList());
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

    /**
     * Gets the internal class for the given path in the document hierarchy
     *
     * @param path the path to map the given Class to
     * @return the class for a given path in the hierarchy
     */
    protected Class<? extends DBObject> getInternalClass(final String path) {
        return objectFactory.getClassForPath(asList(path.split("\\.")));
    }

    @Override
    public String toString() {
        return "DBCollection{database=" + database + ", name='" + name + '\'' + '}';
    }

    synchronized DBObjectFactory getObjectFactory() {
        return this.objectFactory;
    }

    synchronized void setObjectFactory(final DBCollectionObjectFactory factory) {
        this.objectFactory = factory;
        this.objectCodec = new CompoundDBObjectCodec(objectCodec.getEncoder(), getDefaultDBObjectCodec());
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
            return translateBulkWriteResult(executor.execute(new MixedBulkWriteOperation(getNamespace(),
                                                                                         translateWriteRequestsToNew(writeRequests,
                                                                                                                     getObjectCodec()),
                                                                                         ordered, writeConcern)
                                                             .bypassDocumentValidation(bypassDocumentValidation)),
                                            getObjectCodec());
        } catch (MongoBulkWriteException e) {
            throw BulkWriteHelper.translateBulkWriteException(e, MongoClient.getDefaultCodecRegistry().get(DBObject.class));
        }
    }

    DBObjectCodec getDefaultDBObjectCodec() {
        return new DBObjectCodec(MongoClient.getDefaultCodecRegistry(),
                                 DBObjectCodec.getDefaultBsonTypeClassMap(),
                                 getObjectFactory());
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
        if (options.containsField("bucketSize")) {
            request.bucketSize(convertOptionsToType(options, "bucketSize", Double.class));
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

        return new CreateIndexesOperation(getNamespace(), asList(request));
    }

    private String getIndexNameFromIndexFields(final DBObject index) {
        StringBuilder indexName = new StringBuilder();
        for (final String keyNames : index.keySet()) {
            if (indexName.length() != 0) {
                indexName.append('_');
            }
            indexName.append(keyNames).append('_');
            Object keyType = index.get(keyNames);
            if (keyType instanceof Integer) {
                List<Integer> validIndexTypes = asList(1, -1);
                if (!validIndexTypes.contains(keyType)) {
                    throw new UnsupportedOperationException("Unsupported index type: " + keyType);
                }
                indexName.append(((Integer) keyType));
            } else if (keyType instanceof String) {
                List<String> validIndexTypes = asList("2d", "2dsphere", "text", "geoHaystack", "hashed");
                if (!validIndexTypes.contains(keyType)) {
                    throw new UnsupportedOperationException("Unsupported index type: " + keyType);
                }
                indexName.append(((String) keyType).replace(' ', '_'));
            }
        }
        return indexName.toString();
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

    BufferProvider getBufferPool() {
        return getDB().getBufferPool();
    }

    BsonDocument wrapAllowNull(final DBObject document) {
        if (document == null) {
            return null;
        }
        return wrap(document);
    }

    BsonDocument wrap(final DBObject document) {
        return new BsonDocumentWrapper<DBObject>(document, getDefaultDBObjectCodec());
    }

    BsonDocument wrap(final DBObject document, final DBEncoder encoder) {
        if (encoder == null) {
            return wrap(document);
        } else {
            return new BsonDocumentWrapper<DBObject>(document, new DBEncoderAdapter(encoder));
        }
    }

    BsonDocument wrap(final DBObject document, final Encoder<DBObject> encoder) {
        if (encoder == null) {
            return wrap(document);
        } else {
            return new BsonDocumentWrapper<DBObject>(document, encoder);
        }
    }
}
