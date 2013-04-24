/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import com.mongodb.codecs.CollectibleDBObjectCodec;
import com.mongodb.codecs.DBEncoderAdapter;
import org.bson.types.ObjectId;
import org.mongodb.Codec;
import org.mongodb.CollectibleCodec;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.Get;
import org.mongodb.Index;
import org.mongodb.MongoConnector;
import org.mongodb.MongoNamespace;
import org.mongodb.OrderBy;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.codecs.ObjectIdGenerator;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.CollStats;
import org.mongodb.command.Count;
import org.mongodb.command.CountCommandResult;
import org.mongodb.command.Distinct;
import org.mongodb.command.DistinctCommandResult;
import org.mongodb.command.Drop;
import org.mongodb.command.DropIndex;
import org.mongodb.command.FindAndModifyCommandResult;
import org.mongodb.command.FindAndModifyCommandResultCodec;
import org.mongodb.command.FindAndRemove;
import org.mongodb.command.FindAndReplace;
import org.mongodb.command.FindAndUpdate;
import org.mongodb.command.MongoCommand;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.command.MongoDuplicateKeyException;
import org.mongodb.command.RenameCollection;
import org.mongodb.command.RenameCollectionOptions;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFindAndRemove;
import org.mongodb.operation.MongoFindAndReplace;
import org.mongodb.operation.MongoFindAndUpdate;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.QueryResult;
import org.mongodb.util.FieldHelpers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.DBObjects.toDBObject;
import static com.mongodb.DBObjects.toDocument;
import static com.mongodb.DBObjects.toFieldSelectorDocument;
import static com.mongodb.DBObjects.toUpdateOperationsDocument;


/**
 * Implementation of a database collection.
 * <p/>
 * A typical invocation sequence is thus
 * <blockquote>
 * <pre>
 *     MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
 *     DB db = mongo.getDB("mydb");
 *     DBCollection collection = db.getCollection("test");
 * </pre>
 * </blockquote>
 * <p/>
 * To get a collection to use, just specify the name of the collection to the getCollection(String collectionName) method:
 * <blockquote>
 * <pre>
 *     DBCollection coll = db.getCollection("testCollection");
 * </pre>
 * </blockquote>
 * <p/>
 * Once you have the collection object, you can insert documents into the collection:
 * <blockquote>
 * <pre>
 *     BasicDBObject doc = new BasicDBObject("name", "MongoDB")
 *     .append("type", "database")
 *     .append("count", 1)
 *     .append("info", new BasicDBObject("x", 203).append("y", 102));
 *     coll.insert(doc);
 * </pre>
 * </blockquote>
 * <p/>
 * To show that the document we inserted in the previous step is there, we can do a simple findOne() operation to get the first document in the collection:
 * <blockquote>
 * <pre>
 *     DBObject myDoc = coll.findOne();
 *     System.out.println(myDoc);
 * </pre>
 * </blockquote>
 */
@ThreadSafe
@SuppressWarnings({ "rawtypes", "deprecation" })
public class DBCollection implements IDBCollection {
    private static final String NAMESPACE_KEY_NAME = "ns";
    private final DB database;
    private final String name;
    private final Map<String, Class<? extends DBObject>> pathToClassMap;
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    private List<DBObject> hintFields;
    private final Bytes.OptionHolder optionHolder;

    private DBEncoderFactory encoderFactory;
    private DBDecoderFactory decoderFactory;

    private final Codec<Document> documentCodec;
    private CollectibleCodec<DBObject> codec;

    DBCollection(final String name, final DB database, final Codec<Document> documentCodec) {
        this.name = name;
        this.database = database;
        this.documentCodec = documentCodec;
        this.optionHolder = new Bytes.OptionHolder(database.getOptionHolder());
        this.pathToClassMap = new HashMap<String, Class<? extends DBObject>>();
        updateObjectCodec(BasicDBObject.class);
    }

    /**
     * Insert a document into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     *
     * @param document     {@code DBObject} to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final DBObject document, final WriteConcern writeConcern) {
        return insert(Arrays.asList(document), writeConcern);
    }

    /**
     * Insert documents into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     * Collection wide {@code WriteConcern} will be used.
     *
     * @param documents {@code DBObject}'s to be inserted
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final DBObject... documents) {
        return insert(Arrays.asList(documents), getWriteConcern());
    }

    /**
     * Insert documents into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     *
     * @param documents    {@code DBObject}'s to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final WriteConcern writeConcern, final DBObject... documents) {
        return insert(documents, writeConcern);
    }

    /**
     * Insert documents into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     *
     * @param documents    {@code DBObject}'s to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final DBObject[] documents, final WriteConcern writeConcern) {
        return insert(Arrays.asList(documents), writeConcern);
    }

    /**
     * Insert documents into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     *
     * @param documents list of {@code DBObject} to be inserted
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final List<DBObject> documents) {
        return insert(documents, getWriteConcern());
    }

    /**
     * Insert documents into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     *
     * @param documents    list of {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final List<DBObject> documents, final WriteConcern aWriteConcern) {
        final MongoInsert<DBObject> mongoInsert = new MongoInsert<DBObject>(documents)
                .writeConcern(aWriteConcern.toNew());
        return new WriteResult(insertInternal(mongoInsert, codec), aWriteConcern, getDB());
    }

    /**
     * Insert documents into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     *
     * @param documents    {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param encoder      {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final DBObject[] documents, final WriteConcern aWriteConcern, final DBEncoder encoder) {
        return insert(Arrays.asList(documents), aWriteConcern, encoder);
    }

    /**
     * Insert documents into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     *
     * @param documents    a list of {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param dbEncoder      {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final List<DBObject> documents, final WriteConcern aWriteConcern, final DBEncoder dbEncoder) {
        final Encoder<DBObject> encoder = toEncoder(dbEncoder);

        final MongoInsert<DBObject> mongoInsert = new MongoInsert<DBObject>(documents)
                .writeConcern(this.writeConcern.toNew());
        return new WriteResult(insertInternal(mongoInsert, encoder), aWriteConcern, getDB());
    }

    private Encoder<DBObject> toEncoder(DBEncoder dbEncoder) {
        Encoder<DBObject> encoder;
        if (dbEncoder != null) {
            encoder = new DBEncoderAdapter(dbEncoder);
        } else if (encoderFactory != null) {
            encoder = new DBEncoderAdapter(encoderFactory.create());
        } else {
            encoder = this.codec;
        }
        return encoder;
    }

    private org.mongodb.result.WriteResult insertInternal(final MongoInsert<DBObject> mongoInsert, Encoder<DBObject> encoder) {
        try {
            return getConnector().insert(getNamespace(), mongoInsert, encoder);
        } catch (MongoDuplicateKeyException e) {
            throw new MongoException.DuplicateKey(e);
        }
    }

    /**
     * Update an existing document or insert a document depending on the parameter.
     * If the document does not contain an '_id' field, then the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectid value.
     * If the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field:
     * <ul>
     * <li>If a document does not exist with the specified '_id' value, the method performs an insert with the specified fields in the document.</li>
     * <li>If a document exists with the specified '_id' value, the method performs an update, replacing all field in the existing record with the fields from the document.</li>
     * </ul>
     *
     * @param document {@link DBObject} to save to the collection.
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult save(final DBObject document) {
        return save(document, getWriteConcern());
    }

    /**
     * Update an existing document or insert a document depending on the parameter.
     * If the document does not contain an '_id' field, then the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectid value.
     * If the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field:
     * <ul>
     * <li>If a document does not exist with the specified '_id' value, the method performs an insert with the specified fields in the document.</li>
     * <li>If a document exists with the specified '_id' value, the method performs an update, replacing all field in the existing record with the fields from the document.</li>
     * </ul>
     *
     * @param document     {@link DBObject} to save to the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult save(final DBObject document, final WriteConcern writeConcern) {
        final Object id = getCodec().getId(document);
        if (id == null) {
            return insert(document, writeConcern);
        } else {
            return replaceOrInsert(document, writeConcern);
        }
    }

    private WriteResult replaceOrInsert(final DBObject obj, final WriteConcern wc) {
        final Document filter = new Document("_id", getCodec().getId(obj));

        final MongoReplace<DBObject> replace = new MongoReplace<DBObject>(filter, obj)
                .upsert(true)
                .writeConcern(wc.toNew());

        return new WriteResult(getConnector().replace(getNamespace(), replace, getDocumentCodec(),
                getCodec()), wc, getDB());
    }

    /**
     * Modify an existing document or documents in collection.
     * The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query        the selection criteria for the update
     * @param update       the modifications to apply
     * @param upsert       insert a document if no document matches the update query criteria
     * @param multi        update all documents in the collection that match the update query criteria
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     */
    @Override
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern aWriteConcern) {
        if (update == null) {
            throw new IllegalArgumentException("update can not be null");
        }

        if (query == null) {
            throw new IllegalArgumentException("update query can not be null");
        }

        final MongoUpdate mongoUpdate = new MongoUpdate(toDocument(query), toDocument(update))
                .upsert(upsert)
                .multi(multi)
                .writeConcern(aWriteConcern.toNew());

        return new WriteResult(updateInternal(mongoUpdate), aWriteConcern, getDB());
    }

    private org.mongodb.result.WriteResult updateInternal(MongoUpdate mongoUpdate) {
        try {
            return  getConnector().update(getNamespace(), mongoUpdate, documentCodec);
        } catch (org.mongodb.MongoException e) {
            throw new MongoException(e);
        }
    }

    /**
     * Modify an existing document or documents in collection.
     * By default the method updates a single document.
     * The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query        the selection criteria for the update
     * @param update       the modifications to apply
     * @param upsert       insert a document if no document matches the update query criteria
     * @param multi        update all documents in the collection that match the update query criteria
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param encoder      {@code DBEncoder} to be used
     * @return the result of the operation
     */
    @Override
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern aWriteConcern, final DBEncoder encoder) {
        if (update == null) {
            throw new IllegalArgumentException("update can not be null");
        }

        if (query == null) {
            throw new IllegalArgumentException("update query can not be null");
        }

        final Document filter = toDocument(query, encoder, getDocumentCodec());
        final Document updateOperations = toDocument(update, encoder, getDocumentCodec());
        final MongoUpdate mongoUpdate = new MongoUpdate(filter, updateOperations)
                .upsert(upsert)
                .multi(multi)
                .writeConcern(aWriteConcern.toNew());

        return new WriteResult(updateInternal(mongoUpdate), aWriteConcern, getDB());
    }

    /**
     * Modify an existing document or documents in collection.
     * The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @param upsert insert a document if no document matches the update query criteria
     * @param multi  update all documents in the collection that match the update query criteria
     * @return the result of the operation
     */
    @Override
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi) {
        return update(query, update, upsert, multi, getWriteConcern());
    }

    /**
     * Modify an existing document.
     * The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @return the result of the operation
     */
    @Override
    public WriteResult update(final DBObject query, final DBObject update) {
        return update(query, update, false, false);
    }

    /**
     * Modify documents in collection.
     * The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @return the result of the operation
     */
    @Override
    public WriteResult updateMulti(final DBObject query, final DBObject update) {
        return update(query, update, false, true);
    }

    /**
     * Remove documents from a collection.
     *
     * @param query he deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents in the collection.
     * @return the result of the operation
     */
    @Override
    public WriteResult remove(final DBObject query) {
        return remove(query, getWriteConcern());
    }

    /**
     * Remove documents from a collection.
     *
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents in the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     */
    @Override
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern) {

        final MongoRemove mongoRemove = new MongoRemove(toDocument(query))
                .writeConcern(writeConcern.toNew());

        final org.mongodb.result.WriteResult result = getConnector().remove(getNamespace(), mongoRemove, documentCodec);

        return new WriteResult(result, writeConcern, getDB());
    }

    /**
     * Remove documents from a collection.
     *
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents in the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @param encoder      {@code DBEncoder} to be used
     * @return
     */
    @Override
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern, final DBEncoder encoder) {
        final Document filter = toDocument(query, encoder, getDocumentCodec());
        final MongoRemove mongoRemove = new MongoRemove(filter)
                .writeConcern(writeConcern.toNew());

        final org.mongodb.result.WriteResult result = getConnector().remove(getNamespace(), mongoRemove, getDocumentCodec());

        return new WriteResult(result, writeConcern, getDB());
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query      the selection criteria using query operators. Omit the query parameter or pass an empty document to return all documents in the collection.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @param numToSkip  number of documents to skip
     * @param batchSize  see {@link DBCursor#batchSize(int)} for more information
     * @param options    query options to be used
     * @return A cursor to the documents that match the query criteria
     */
    @Override
    public DBCursor find(final DBObject query, final DBObject projection, final int numToSkip, final int batchSize,
                         final int options) {
        return new DBCursor(this, query, projection, getReadPreference())
                .batchSize(batchSize)
                .skip(numToSkip)
                .setOptions(options);
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query      the selection criteria using query operators. Omit the query parameter or pass an empty document to return all documents in the collection.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @param numToSkip  number of documents to skip
     * @param batchSize  see {@link DBCursor#batchSize(int)} for more information
     * @return A cursor to the documents that match the query criteria
     */
    @Override
    public DBCursor find(final DBObject query, final DBObject projection, final int numToSkip, final int batchSize) {
        return new DBCursor(this, query, projection, getReadPreference())
                .batchSize(batchSize)
                .skip(numToSkip);
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query the selection criteria using query operators. Omit the query parameter or pass an empty document to return all documents in the collection.
     * @return A cursor to the documents that match the query criteria
     */
    @Override
    public DBCursor find(final DBObject query) {
        return new DBCursor(this, query, null, getReadPreference());
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query      the selection criteria using query operators. Omit the query parameter or pass an empty document to return all documents in the collection.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @return A cursor to the documents that match the query criteria
     */
    @Override
    public DBCursor find(final DBObject query, final DBObject projection) {
        return new DBCursor(this, query, projection, getReadPreference());
    }

    /**
     * Select all documents in collection and get a cursor to the selected documents.
     *
     * @return A cursor to the documents that match the query criteria
     */
    @Override
    public DBCursor find() {
        return find(new BasicDBObject());
    }

    /**
     * Get a single document from collection.
     *
     * @return A document that satisfies the query specified as the argument to this method.
     */
    @Override
    public DBObject findOne() {
        return findOne(new BasicDBObject());
    }

    /**
     * Get a single document from collection.
     *
     * @param query the selection criteria using query operators.
     * @return A document that satisfies the query specified as the argument to this method.
     */
    @Override
    public DBObject findOne(final DBObject query) {
        return findOne(query, null, null, getReadPreference());
    }

    /**
     * Get a single document from collection.
     *
     * @param query      the selection criteria using query operators.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     */
    @Override
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
     */
    @Override
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
     */
    @Override
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
     */
    @Override
    public DBObject findOne(final DBObject query, final DBObject projection, final DBObject sort,
                            final ReadPreference readPreference) {

        final MongoFind mongoFind = new MongoFind()
                .select(toFieldSelectorDocument(projection))
                .where(toDocument(query))
                .order(toDocument(sort))
                .readPreference(readPreference.toNew())
                .batchSize(-1);

        final QueryResult<DBObject> res = getConnector().query(getNamespace(), mongoFind,
                documentCodec, getCodec());
        if (res.getResults().isEmpty()) {
            return null;
        }

        return res.getResults().get(0);
    }

    /**
     * Get a single document from collection by '_id'.
     *
     * @param id value of '_id' field of a document we are looking for
     * @return A document with '_id' provided as the argument to this method.
     */
    @Override
    public DBObject findOne(final Object id) {
        return findOne(id, null);
    }

    /**
     * Get a single document from collection by '_id'.
     *
     * @param id         value of '_id' field of a document we are looking for
     * @param projection specifies which projection MongoDB will return from the documents in the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     */
    @Override
    public DBObject findOne(final Object id, final DBObject projection) {
        return findOne(new BasicDBObject("_id", id), projection);
    }

    /**
     * Template method pattern.
     * Please extend DBCollection and ovverride {@link #doapply(DBObject)} if you need to add specific fields before saving object to collection.
     *
     * @param document document to be passed to {@code doapply()}
     * @return '_id' of the document
     */
    @Override
    public Object apply(final DBObject document) {
        return apply(document, true);
    }

    /**
     * Template method pattern.
     * Please extend DBCollection and ovverride {@link #doapply(DBObject)} if you need to add specific fields before saving object to collection.
     *
     * @param document document to be passed to {@code doapply()}
     * @param ensureId specifies if '_id' field needs to be added to the document in case of absence.
     * @return '_id' of the document
     */
    @Override
    public Object apply(final DBObject document, final boolean ensureId) {
        Object id = document.get("_id");
        if (ensureId && id == null) {
            id = ObjectId.get();
            document.put("_id", id);
        }

        doapply(document);

        return id;
    }

    /**
     * Method to be overridden if you need to add any fields to a given document before saving it to the collection.
     *
     * @param document object to which to add the fields
     */
    protected void doapply(final DBObject document) {
    }

    /**
     * Same as {@link #getCount()}
     *
     * @return the number of documents in collection
     * @throws MongoException
     */
    @Override
    public long count() {
        return getCount(new BasicDBObject(), null);
    }

    /**
     * Same as {@link #getCount(DBObject)}
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     */
    @Override
    public long count(final DBObject query) {
        return getCount(query, null);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query          specifies the selection criteria
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     */
    @Override
    public long count(final DBObject query, final ReadPreference readPreference) {
        return getCount(query, null, readPreference);
    }

    /**
     * Get the count of documents in collection.
     *
     * @return the number of documents in collection
     * @throws MongoException
     */
    @Override
    public long getCount() {
        return getCount(new BasicDBObject(), null);
    }

    /**
     * Get the count of documents in collection.
     *
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the number of documents in collection
     * @throws MongoException
     */
    @Override
    public long getCount(final ReadPreference readPreference) {
        return getCount(new BasicDBObject(), null, readPreference);
    }


    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     */
    @Override
    public long getCount(final DBObject query) {
        return getCount(query, null);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query      specifies the selection criteria
     * @param projection this is ignored
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     */
    @Override
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
     * @throws MongoException
     */
    @Override
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
     * @throws MongoException
     */
    @Override
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
     * @throws MongoException
     */
    @Override
    public long getCount(final DBObject query, final DBObject projection, final long limit, final long skip,
                         final ReadPreference readPreference) {
        if (limit > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("limit is too large: " + limit);
        }

        if (skip > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("skip is too large: " + skip);
        }

        // TODO: investigate case of int to long for skip
        final Count countCommand = new Count(new MongoFind(toDocument(query)), getName());
        countCommand.limit((int) limit).skip((int) skip).readPreference(readPreference.toNew());


        return new CountCommandResult(getDB().executeCommand(countCommand)).getCount();
    }

    /**
     * Change the name of an existing collection.
     *
     * @param newName specifies the new name of the collection
     * @return the collection with new name
     * @throws MongoException if target is the name of an existing collection.
     */
    @Override
    public DBCollection rename(final String newName) {
        return rename(newName, false);
    }

    /**
     * Change the name of an existing collection.
     *
     * @param newName    specifies the new name of the collection
     * @param dropTarget If {@code true}, mongod will drop the collection with the target name in case it exists
     * @return the collection with new name
     * @throws MongoException if target is the name of an existing collection and {@code dropTarget=false}.
     */
    @Override
    public DBCollection rename(final String newName, final boolean dropTarget) {

        final RenameCollectionOptions renameCollectionOptions = new RenameCollectionOptions(getName(), newName, dropTarget);
        final RenameCollection renameCommand = new RenameCollection(renameCollectionOptions, getDB().getName());
        try {
            getConnector().command("admin", renameCommand, getDocumentCodec());
            return getDB().getCollection(newName);
        } catch (org.mongodb.MongoException e) {
            throw new MongoException(e);
        }
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
     */
    @Override
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
     */
    @Override
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
     */
    @Override
    public DBObject group(final DBObject key, final DBObject cond, final DBObject initial, final String reduce,
                          final String finalize, final ReadPreference readPreference) {
        throw new UnsupportedOperationException(); //TODO We neew GroupCommand to implement this
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param cmd the group command
     * @return a document with the grouped records as well as the command meta-data
     */
    @Override
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
     */
    @Override
    public DBObject group(final GroupCommand cmd, final ReadPreference readPreference) {
        throw new UnsupportedOperationException(); //TODO We neew GroupCommand to implement this
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * Deprecated. Use {@link #group(com.mongodb.IDBCollection.GroupCommand)} instead.
     *
     * @param args specifies the arguments to the group function
     * @return a document with the grouped records as well as the command meta-data
     */
    @Override
    @Deprecated
    public DBObject group(final DBObject args) {
        throw new UnsupportedOperationException(); //TODO We neew GroupCommand to implement this
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName Specifies the field for which to return the distinct values.
     * @return an array of the distinct values
     */
    @Override
    public List distinct(final String fieldName) {
        return distinct(fieldName, getReadPreference());
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName      Specifies the field for which to return the distinct values
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return an array of the distinct values
     */
    @Override
    public List distinct(final String fieldName, final ReadPreference readPreference) {
        return distinct(fieldName, new BasicDBObject(), readPreference);
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName Specifies the field for which to return the distinct values
     * @param query     specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @return an array of the distinct values
     */
    @Override
    public List distinct(final String fieldName, final DBObject query) {
        return distinct(fieldName, query, getReadPreference());
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName      Specifies the field for which to return the distinct values
     * @param query          specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return an array of the distinct values
     */
    @Override
    public List distinct(final String fieldName, final DBObject query, final ReadPreference readPreference) {
        final MongoFind mongoFind = new MongoFind()
                .filter(toDocument(query))
                .readPreference(readPreference.toNew());
        final Distinct distinctOperation = new Distinct(getName(), fieldName, mongoFind);
        return new DistinctCommandResult(getDB().executeCommand(distinctOperation)).getValue();
    }

    /**
     * Perform mapReduce operation.
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation.
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return a mapReduce output
     */
    @Override
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final DBObject query) {
        final MapReduceCommand command = new MapReduceCommand(this, map, reduce, outputTarget, MapReduceCommand.OutputType.REDUCE, query);
        return mapReduce(command);
    }

    /**
     * Perform mapReduce operation.
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation.
     * @param outputType   specifies the type of job output
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return a mapReduce output
     */
    @Override
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final MapReduceCommand.OutputType outputType, final DBObject query) {
        final MapReduceCommand command = new MapReduceCommand(this, map, reduce, outputTarget, outputType, query);
        return mapReduce(command);
    }

    /**
     * Perform mapReduce operation.
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation.
     * @param outputType   specifies the type of job output
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return a mapReduce output
     */
    @Override
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final MapReduceCommand.OutputType outputType, final DBObject query,
                                     final ReadPreference readPreference) {
        final MapReduceCommand command = new MapReduceCommand(this, map, reduce, outputTarget, outputType, query);
        command.setReadPreference(readPreference);
        return mapReduce(command);
    }

    /**
     * Perform mapReduce operation.
     *
     * @param command specifies the command parameters
     * @return a mapReduce output
     */
    @Override
    public MapReduceOutput mapReduce(final MapReduceCommand command) {
        //TODO Check that implementation is correct.
        final DBObject cmd = command.toDBObject();
        // if type in inline, then query options like slaveOk is fine
        final CommandResult res;
        if (command.getOutputType() == MapReduceCommand.OutputType.INLINE) {
            res = database.command(cmd, getOptions(),
                    command.getReadPreference() != null ? command.getReadPreference()
                            : getReadPreference());
        } else {
            res = database.command(cmd);
        }
        res.throwOnError();
        return new MapReduceOutput(this, cmd, res);
    }

    /**
     * Perform mapReduce operation.
     *
     * @param command specifies the command parameters
     * @return a mapReduce output
     */
    @Override
    public MapReduceOutput mapReduce(final DBObject command) {
        throw new UnsupportedOperationException(); //TODO DBObject needs to be converted to mapReduceCommand or directly sent to db.executeCommand
    }

    /**
     * Method implements aggregation framework.
     *
     * @param firstOp       requisite first operation to be performed in the aggregation pipeline
     * @param additionalOps additional operations to be performed in the aggregation pipeline
     * @return the aggregation operation's result set
     */
    @Override
    public AggregationOutput aggregate(final DBObject firstOp, final DBObject... additionalOps) {
        throw new UnsupportedOperationException("Not implemented yet."); //TODO: We need AggregationCommand to implement this.
    }

    /**
     * Get the name of a collection.
     *
     * @return the name of a collection
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Get the full name of a collection, with the database name as a prefix.
     *
     * @return the name of a collection
     */
    @Override
    public String getFullName() {
        return getNamespace().getFullName();
    }

    /**
     * Find a collection that is prefixed with this collection's name. A typical use of this might be
     * <blockquote><pre>
     *    DBCollection users = mongo.getCollection( "wiki" ).getCollection( "users" );
     * </pre></blockquote>
     * Which is equivalent to
     * <pre><blockquote>
     *   DBCollection users = mongo.getCollection( "wiki.users" );
     * </pre></blockquote>
     *
     * @param name the name of the collection to find
     * @return the matching collection
     */
    @Override
    public DBCollection getCollection(final String name) {
        return database.getCollection(getName() + "." + name);
    }

    /**
     * Creates an index on the field specified, if that index does not already exist.
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     */
    @Override
    public void ensureIndex(final DBObject keys) {
        ensureIndex(keys, (DBObject) null);
    }

    /**
     * Creates an index on the field specified, if that index does not already exist.
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     * @param name specifies the name of the index
     */
    @Override
    public void ensureIndex(final DBObject keys, final String name) {
        final BasicDBObject options = new BasicDBObject("name", name);
        ensureIndex(keys, options);
    }

    /**
     * Creates an index on the field specified, if that index does not already exist.
     *
     * @param keys   a document that contains pairs with the name of the field or fields to index and order of the index
     * @param name   specifies the name of the index
     * @param unique specify true to create a unique index so that the collection will not accept insertion of documents where the index key or keys matches an existing value in the index
     */
    @Override
    public void ensureIndex(final DBObject keys, final String name, final boolean unique) {
        final BasicDBObject options = new BasicDBObject("name", name);
        options.append("unique", unique);
        ensureIndex(keys, options);
    }

    /**
     * Creates an index on the field specified, if that index does not already exist.
     *
     * @param keys    a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options a document that controls the creation of the index.
     */
    @Override
    public void ensureIndex(final DBObject keys, final DBObject options) {
        final MongoInsert<Document> insertIndexOperation
                = new MongoInsert<Document>(toIndexDetailsDocument(keys, options));
        insertIndex(insertIndexOperation, documentCodec);
    }

    /**
     * Creates an ascending index on the field specified with default options, if that index does not already exist.
     *
     * @param name specifies name of field to index on
     */
    @Override
    public void ensureIndex(final String name) {
        final Index index = new Index(new Index.OrderedKey(name, OrderBy.ASC));
        final Document indexDetails = index.toDocument();
        indexDetails.append(NAMESPACE_KEY_NAME, getNamespace().getFullName());
        final MongoInsert<Document> insertIndexOperation
                = new MongoInsert<Document>(indexDetails);
        insertIndex(insertIndexOperation, documentCodec);
    }

    /**
     * Deprecated. The {@link #ensureIndex(DBObject)} method is the preferred way to create indexes on collections.
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     */
    @Override
    public void createIndex(final DBObject keys) {
        ensureIndex(keys);
    }

    /**
     * Deprecated. The {@link #ensureIndex(DBObject)} method is the preferred way to create indexes on collections.
     *
     * @param keys    a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options a document that controls the creation of the index.
     */
    @Override
    public void createIndex(final DBObject keys, final DBObject options) {
        ensureIndex(keys, options);
    }

    /**
     * Deprecated. The {@link #ensureIndex(DBObject)} method is the preferred way to create indexes on collections.
     *
     * @param keys    a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options a document that controls the creation of the index.
     * @param dbEncoder specifies the encoder that used during operation
     */
    @Override
    public void createIndex(final DBObject keys, final DBObject options, final DBEncoder dbEncoder) {

        final Encoder<DBObject> encoder = toEncoder(dbEncoder);
        final Document indexDetails = toIndexDetailsDocument(keys, options);

        final MongoInsert<DBObject> insertIndexOperation = new MongoInsert<DBObject>(toDBObject(indexDetails));
        insertIndex(insertIndexOperation, encoder);
    }

    private <T> void insertIndex(final MongoInsert<T> insertIndexOperation, final Encoder<T> encoder) {
        insertIndexOperation.writeConcern(org.mongodb.WriteConcern.ACKNOWLEDGED);
        try {
            getConnector().insert(new MongoNamespace(getDB().getName(), "system.indexes"), insertIndexOperation, encoder);
        } catch (MongoDuplicateKeyException exception) {
            throw new MongoException.DuplicateKey(exception);
        }
    }

    /**
     * Clears all indices that have not yet been applied to this collection.
     */
    @Override
    public void resetIndexCache() {
    }

    /**
     * Override MongoDB's default index selection and query optimization process.
     *
     * @param indexes list of indexes to "hint" or force MongoDB to use when performing the query.
     */
    @Override
    public void setHintFields(final List<DBObject> indexes) {
        hintFields = indexes;
    }


    /**
     * Atomically modify and return a single document.
     * By default, the returned document does not include the modifications made on the update.
     *
     * @param query  specifies the selection criteria for the modification
     * @param sort   determines which document the operation will modify if the query selects multiple documents
     * @param update performs an update of the selected document
     * @return pre-modification document
     */
    @Override
    public DBObject findAndModify(final DBObject query, final DBObject sort, final DBObject update) {
        return findAndModify(query, null, sort, false, update, false, false);
    }


    /**
     * Atomically modify and return a single document.
     * By default, the returned document does not include the modifications made on the update.
     *
     * @param query  specifies the selection criteria for the modification
     * @param update performs an update of the selected document
     * @return pre-modification document
     */
    @Override
    public DBObject findAndModify(final DBObject query, final DBObject update) {
        return findAndModify(query, null, null, false, update, false, false);
    }


    /**
     * Atomically modify and return a single document.
     * By default, the returned document does not include the modifications made on the update.
     *
     * @param query specifies the selection criteria for the modification
     * @return pre-modification document
     */
    @Override
    public DBObject findAndRemove(final DBObject query) {
        return findAndModify(query, null, null, true, null, false, false);
    }

    /**
     * Atomically modify and return a single document.
     * By default, the returned document does not include the modifications made on the update.
     *
     * @param query     specifies the selection criteria for the modification
     * @param fields    a subset of fields to return
     * @param sort      determines which document the operation will modify if the query selects multiple documents
     * @param remove    when {@code true}, removes the selected document
     * @param returnNew when true, returns the modified document rather than the original
     * @param update    performs an update of the selected document
     * @param upsert    when true, operation creates a new document if the query returns no documents
     * @return pre-modification document
     */
    @Override
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, final DBObject update,
                                  final boolean returnNew, final boolean upsert) {
        final MongoCommand mongoCommand;

        if (remove) {
            final MongoFindAndRemove<DBObject> mongoFindAndRemove = new MongoFindAndRemove<DBObject>()
                    .where(toDocument(query))
                    .sortBy(toDocument(sort))
                    .returnNew(returnNew);
            mongoCommand = new FindAndRemove<DBObject>(mongoFindAndRemove, getName());
        } else {
            if (update == null) {
                throw new IllegalArgumentException("Update document can't be null");
            }
            if (!update.keySet().isEmpty() && update.keySet().iterator().next().charAt(0) == '$') {

                final MongoFindAndUpdate<DBObject> mongoFindAndUpdate = new MongoFindAndUpdate<DBObject>()
                        .where(toDocument(query))
                        .sortBy(toDocument(sort))
                        .returnNew(returnNew)
                        .select(toDocument(fields))
                        .updateWith(toUpdateOperationsDocument(update))
                        .upsert(upsert);
                mongoCommand = new FindAndUpdate<DBObject>(mongoFindAndUpdate, getName());
            } else {
                final MongoFindAndReplace<DBObject> mongoFindAndReplace
                        = new MongoFindAndReplace<DBObject>(update)
                        .where(toDocument(query))
                        .sortBy(toDocument(sort))
                        .select(toDocument(fields))
                        .returnNew(returnNew)
                        .upsert(upsert);
                mongoCommand = new FindAndReplace<DBObject>(mongoFindAndReplace, getName());
            }
        }

        final FindAndModifyCommandResultCodec<DBObject> findAndModifyCommandResultCodec =
                new FindAndModifyCommandResultCodec<DBObject>(getPrimitiveCodecs(), getCodec());

        final FindAndModifyCommandResult<DBObject> commandResult =
                new FindAndModifyCommandResult<DBObject>(getConnector().command(getDB().getName(), mongoCommand, findAndModifyCommandResultCodec));

        return commandResult.getValue();
    }

    private Get asGetOrder(final boolean returnNew) {
        return returnNew ? Get.BeforeChangeApplied : Get.AfterChangeApplied;
    }

    /**
     * Returns the database this collection is a member of.
     *
     * @return this collection's database
     */
    @Override
    public DB getDB() {
        return database;
    }

    @Override
    public Class getObjectClass() {
        return codec.getEncoderClass();
    }

    /**
     * Sets a default class for objects in this collection; null resets the class to nothing.
     *
     * @param objectClass the class
     */
    public synchronized void setObjectClass(final Class<? extends DBObject> objectClass) {
        updateObjectCodec(objectClass);
    }

    /**
     * Get the {@link WriteConcern} for this collection.
     *
     * @return WriteConcern value
     */
    @Override
    public WriteConcern getWriteConcern() {
        if (writeConcern != null) {
            return writeConcern;
        }
        return database.getWriteConcern();
    }

    /**
     * Set the {@link WriteConcern} for this collection. Will be used for writes to this collection. Overrides any setting of
     * write concern at the DB level.
     *
     * @param writeConcern WriteConcern to use
     */
    @Override
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the {@link ReadPreference}.
     *
     * @return ReadPreference value
     */
    @Override
    public ReadPreference getReadPreference() {
        if (readPreference != null) {
            return readPreference;
        }
        return database.getReadPreference();
    }

    /**
     * Sets the {@link ReadPreference} for this collection. Will be used as default for reads from this collection; overrides
     * DB & Connection level settings. See the * documentation for {@link ReadPreference} for more information.
     *
     * @param preference ReadPreference to use
     */
    @Override
    public void setReadPreference(final ReadPreference preference) {
        this.readPreference = preference;
    }

    @Override
    @Deprecated
    public void slaveOk() {
        addOption(Bytes.QUERYOPTION_SLAVEOK);
    }

    @Override
    public void addOption(final int option) {
        optionHolder.add(option);
    }


    @Override
    public void resetOptions() {
        optionHolder.reset();
    }

    @Override
    public int getOptions() {
        return optionHolder.get();
    }

    @Override
    public void setOptions(final int options) {
        optionHolder.set(options);
    }

    /**
     * Drops (deletes) this collection from the database. Use with care.
     *
     * @throws MongoException
     */
    @Override
    public void drop() {
        try {
            org.mongodb.result.CommandResult commandResult = getDB().executeCommand(new Drop(getName()));
        } catch (MongoCommandFailureException ex) {
            if (!"ns not found".equals(ex.getErrorMessage())) {
                throw new MongoException(ex);
            }
        }
    }

    @Override
    public DBDecoderFactory getDBDecoderFactory() {
        return decoderFactory;
    }

    @Override
    public void setDBDecoderFactory(final DBDecoderFactory factory) {
        this.decoderFactory = factory;
    }

    @Override
    public DBEncoderFactory getDBEncoderFactory() {
        return this.encoderFactory;
    }

    @Override
    public void setDBEncoderFactory(final DBEncoderFactory factory) {
        this.encoderFactory = factory;
    }

    /**
     * Return a list of the indexes for this collection.  Each object in the list is the "info document" from MongoDB
     *
     * @return list of index documents
     * @throws MongoException
     */
    @Override
    public List<DBObject> getIndexInfo() {
        final ArrayList<DBObject> res = new ArrayList<DBObject>();

        final MongoFind queryForCollectionNamespace = new MongoFind(
                new Document(NAMESPACE_KEY_NAME, getNamespace().getFullName()))
                .readPreference(org.mongodb.ReadPreference.primary());

        final QueryResult<Document> systemCollection = getConnector().query(
                new MongoNamespace(database.getName(), "system.indexes"),
                queryForCollectionNamespace,
                documentCodec, documentCodec);

        final List<Document> indexes = systemCollection.getResults();
        for (final Document curIndex : indexes) {
            res.add(DBObjects.toDBObject(curIndex));
        }
        return res;
    }

    @Override
    public void dropIndex(final DBObject keys) {
        final List<Index.Key> keysFromDBObject = getKeysFromDBObject(keys);
        final Index indexToDrop = new Index(keysFromDBObject.toArray(new Index.Key[keysFromDBObject.size()]));
        final DropIndex dropIndex = new DropIndex(getName(), indexToDrop.getName());
        getDB().executeCommand(dropIndex);
    }

    @Override
    public void dropIndex(final String name) {
        final DropIndex dropIndex = new DropIndex(getName(), name);
        getDB().executeCommand(dropIndex);
        //TODO: currently doesn't deal with errors
    }

    @Override
    public void dropIndexes() {
        dropIndexes("*");
    }

    @Override
    public void dropIndexes(final String name) {
        dropIndex(name);
    }

    @Override
    public CommandResult getStats() {
        final org.mongodb.result.CommandResult commandResult = getDB().executeCommand(new CollStats(getName()));
        return new CommandResult(commandResult);
    }

    @Override
    public boolean isCapped() {
        final CommandResult commandResult = getStats();
        final Object cappedField = commandResult.get("capped");
        return cappedField != null && (cappedField.equals(1) || cappedField.equals(true));
    }

    /**
     * Sets the internal class for the given path in the document hierarchy
     *
     * @param path  the path to map the given Class to
     * @param clazz the Class to map the given path to
     */
    public synchronized void setInternalClass(final String path, final Class<? extends DBObject> clazz) {
        pathToClassMap.put(path, clazz);
    }

    private void updateObjectCodec(final Class<? extends DBObject> objectClass) {
        final HashMap<String, Class<? extends DBObject>> map = new HashMap<String, Class<? extends DBObject>>(pathToClassMap);
        this.codec = new CollectibleDBObjectCodec(database,
                getPrimitiveCodecs(), new ObjectIdGenerator(), objectClass, map);
    }

    private PrimitiveCodecs getPrimitiveCodecs() {
        return PrimitiveCodecs.createDefault();
    }

    private Document toIndexDetailsDocument(DBObject keys, DBObject options) {
        // TODO: Check if these are all the supported options. driver:index still not supports all options.
        // Waiting for https://trello.com/card/additional-index-functionality-that-isn-t-supported-yet/50c237ecaad6596f2f001a3e/127
        String name = null;
        boolean unique = false;

        if (options != null) {
            if (options.get("name") != null) {
                name = (String) options.get("name");
            }
            if (options.get("unique") != null) {
                unique = FieldHelpers.asBoolean(options.get("unique"));
            }
        }
        final List<Index.Key> keyList = getKeysFromDBObject(keys);
        final Index index = new Index(name, unique, keyList.toArray(new Index.Key[keyList.size()]));
        final Document indexDetails = index.toDocument();
        indexDetails.append(NAMESPACE_KEY_NAME, getNamespace().getFullName());
        return indexDetails;
    }

    private List<Index.Key> getKeysFromDBObject(final DBObject fields) {
        final List<Index.Key> keys = new ArrayList<Index.Key>();
        for (final String key : fields.keySet()) {
            final Object keyType = fields.get(key);
            if (keyType instanceof Integer) {
                keys.add(new Index.OrderedKey(key, OrderBy.fromInt((Integer) fields.get(key))));
            } else if (keyType.equals("2d")) {
                keys.add(new Index.GeoKey(key));
            } else {
                throw new UnsupportedOperationException("Unsupported index type: " + keyType);
            }

        }
        return keys;
    }

    MongoConnector getConnector() {
        return getDB().getConnector();
    }

    CollectibleCodec<DBObject> getCodec() {
        return codec;
    }

    MongoNamespace getNamespace() {
        return new MongoNamespace(getDB().getName(), getName());
    }

    Codec<Document> getDocumentCodec() {
        return documentCodec;
    }

    Bytes.OptionHolder getOptionHolder() {
        return optionHolder;
    }
}