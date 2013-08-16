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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.mongodb.codecs.CompoundDBObjectCodec;
import com.mongodb.codecs.DBEncoderFactoryAdapter;
import com.mongodb.codecs.DocumentCodec;
import org.bson.types.ObjectId;
import org.mongodb.Codec;
import org.mongodb.CollectibleCodec;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.Index;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.Operation;
import org.mongodb.OrderBy;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.codecs.ObjectIdGenerator;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.CollStats;
import org.mongodb.command.Command;
import org.mongodb.command.CountOperation;
import org.mongodb.command.Distinct;
import org.mongodb.command.DistinctCommandResult;
import org.mongodb.command.Drop;
import org.mongodb.command.DropIndex;
import org.mongodb.command.FindAndModifyCommandResult;
import org.mongodb.command.FindAndModifyCommandResultCodec;
import org.mongodb.command.GroupCommandResult;
import org.mongodb.command.MapReduceCommandResult;
import org.mongodb.command.MapReduceCommandResultCodec;
import org.mongodb.command.MapReduceInlineCommandResult;
import org.mongodb.command.RenameCollection;
import org.mongodb.command.RenameCollectionOptions;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.CommandOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.FindAndRemove;
import org.mongodb.operation.FindAndReplace;
import org.mongodb.operation.FindAndUpdate;
import org.mongodb.operation.Insert;
import org.mongodb.operation.InsertOperation;
import org.mongodb.operation.QueryOperation;
import org.mongodb.operation.Remove;
import org.mongodb.operation.RemoveOperation;
import org.mongodb.operation.Replace;
import org.mongodb.operation.ReplaceOperation;
import org.mongodb.operation.Update;
import org.mongodb.operation.UpdateOperation;
import org.mongodb.session.Session;
import org.mongodb.util.FieldHelpers;
import com.mongodb.codecs.CollectibleDBObjectCodec;
import com.mongodb.codecs.DBDecoderAdapter;
import com.mongodb.codecs.DBEncoderAdapter;

import static com.mongodb.DBObjects.toDBList;
import static com.mongodb.DBObjects.toDBObject;
import static com.mongodb.DBObjects.toDocument;
import static com.mongodb.DBObjects.toFieldSelectorDocument;
import static com.mongodb.DBObjects.toNullableDocument;
import static com.mongodb.DBObjects.toUpdateOperationsDocument;
import static com.mongodb.MongoExceptions.mapException;


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
 * To show that the document we inserted in the previous step is there, we can do a simple findOne() operation to get the first document
 * in the collection:
 * <blockquote>
 * <pre>
 *     DBObject myDoc = coll.findOne();
 *     System.out.println(myDoc);
 * </pre>
 * </blockquote>
 */
@ThreadSafe
@SuppressWarnings({"rawtypes", "deprecation"})
public class DBCollection implements IDBCollection {
    private static final String NAMESPACE_KEY_NAME = "ns";
    private final DB database;
    private final String name;
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    private List<DBObject> hintFields;
    private final Bytes.OptionHolder optionHolder;

    private DBEncoderFactory encoderFactory;
    private DBDecoderFactory decoderFactory;
    private DBObjectFactory objectFactory;

    private final Codec<Document> documentCodec;
    private CompoundDBObjectCodec objectCodec;


    /**
     * Constructs new {@code DBCollection} instance. This opertation not reflected on the server.
     *
     * @param name the name of the collection
     * @param database the database to which this collections belongs to
     * @param documentCodec codec to be used for messages to server
     */
    DBCollection(final String name, final DB database, final Codec<Document> documentCodec) {
        this.name = name;
        this.database = database;
        this.documentCodec = documentCodec;
        this.optionHolder = new Bytes.OptionHolder(database.getOptionHolder());
        this.objectFactory = new DBObjectFactory();
        this.objectCodec = new CompoundDBObjectCodec(createCollectibleDBObjectCodec());
    }

    /**
     * Initializes a new collection. No operation is actually performed on the database.
     * @param database database in which to create the collection
     * @param name the name of the collection
     */
    protected DBCollection(final DB database, final String name) {
        this(name, database, new DocumentCodec(PrimitiveCodecs.createDefault()));
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
     * @param documents     list of {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final List<DBObject> documents, final WriteConcern aWriteConcern) {
        final Insert<DBObject> insert = new Insert<DBObject>(aWriteConcern.toNew(), documents);
        return new WriteResult(insert(insert, objectCodec), aWriteConcern);
    }

    /**
     * Insert documents into a collection.
     * If the collection does not exists on the server, then it will be created.
     * If the new document does not contain an '_id' field, it will be added.
     *
     * @param documents     {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param encoder       {@code DBEncoder} to be used
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
     * @param documents     a list of {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param dbEncoder     {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws MongoException if the operation fails
     */
    @Override
    public WriteResult insert(final List<DBObject> documents, final WriteConcern aWriteConcern, final DBEncoder dbEncoder) {
        final Encoder<DBObject> encoder = toEncoder(dbEncoder);

        final Insert<DBObject> insert = new Insert<DBObject>(aWriteConcern.toNew(), documents);
        return new WriteResult(insert(insert, encoder), aWriteConcern);
    }

    private Encoder<DBObject> toEncoder(final DBEncoder dbEncoder) {
        return dbEncoder != null ? new DBEncoderAdapter(dbEncoder, new ObjectIdGenerator()) : objectCodec;
    }

    private CommandResult insert(final Insert<DBObject> insert, final Encoder<DBObject> encoder) {
        try {
            return translateCommandResult(new InsertOperation<DBObject>(getNamespace(), insert, encoder, getBufferPool(), getSession(),
                    false).execute());
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
    }

    private CommandResult translateCommandResult(final org.mongodb.CommandResult commandResult) {
        if (commandResult == null) {
            return null;
        }

        return new CommandResult(commandResult);
    }

    /**
     * Update an existing document or insert a document depending on the parameter.
     * If the document does not contain an '_id' field, then the method performs an insert with the specified fields in the document as
     * well as an '_id' field with a unique objectid value.
     * If the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field:
     * <ul>
     * <li>If a document does not exist with the specified '_id' value, the method performs an insert with the specified fields in the
     * document.</li>
     * <li>If a document exists with the specified '_id' value, the method performs an update, replacing all field in the existing record
     * with the fields from the document.</li>
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
        try {
            final Object id = getObjectCodec().getId(document);
            if (id == null) {
                return insert(document, writeConcern);
            }
            else {
                return replaceOrInsert(document, writeConcern);
            }
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
    }

    private WriteResult replaceOrInsert(final DBObject obj, final WriteConcern wc) {
        try {
            final Document filter = new Document("_id", getObjectCodec().getId(obj));

            final Replace<DBObject> replace = new Replace<DBObject>(wc.toNew(), filter, obj).upsert(true);

            return new WriteResult(translateCommandResult(new ReplaceOperation<DBObject>(getNamespace(), replace, getDocumentCodec(),
                    getObjectCodec(), getBufferPool(), getSession(), false).execute()), wc);
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
    }

    /**
     * Modify an existing document or documents in collection.
     * The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query         the selection criteria for the update
     * @param update        the modifications to apply
     * @param upsert        insert a document if no document matches the update query criteria
     * @param multi         update all documents in the collection that match the update query criteria
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

        final Update mongoUpdate = new Update(aWriteConcern.toNew(), toDocument(query), toDocument(update))
                .upsert(upsert)
                .multi(multi);

        return new WriteResult(updateInternal(mongoUpdate), aWriteConcern);
    }

    /**
     * Modify an existing document or documents in collection.
     * By default the method updates a single document.
     * The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query         the selection criteria for the update
     * @param update        the modifications to apply
     * @param upsert        insert a document if no document matches the update query criteria
     * @param multi         update all documents in the collection that match the update query criteria
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param encoder       {@code DBEncoder} to be used
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
        final Update mongoUpdate = new Update(aWriteConcern.toNew(), filter, updateOperations)
                .upsert(upsert)
                .multi(multi);

        return new WriteResult(updateInternal(mongoUpdate), aWriteConcern);
    }

    private CommandResult updateInternal(final Update update) {
        try {
            return translateCommandResult(new UpdateOperation(getNamespace(), update, documentCodec, getBufferPool(), getSession(),
                    false).execute());
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
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
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                     documents in the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     */
    @Override
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern) {
        final Remove remove = new Remove(writeConcern.toNew(), toDocument(query));
        try {
            return new WriteResult(translateCommandResult(new RemoveOperation(getNamespace(), remove, documentCodec,
                    getBufferPool(), getSession(), false).execute()), writeConcern);
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
    }

    /**
     * Remove documents from a collection.
     *
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                     documents in the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @param encoder      {@code DBEncoder} to be used
     * @return the result of the operation
     */
    @Override
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern, final DBEncoder encoder) {
        final Document filter = toDocument(query, encoder, getDocumentCodec());
        final Remove remove = new Remove(writeConcern.toNew(), filter);

        try {
            return new WriteResult(translateCommandResult(
                    new RemoveOperation(getNamespace(), remove, getDocumentCodec(), getBufferPool(), getSession(), false).execute()),
                    writeConcern);
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
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
     */
    @Override
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
     */
    @Override
    public DBCursor find(final DBObject query, final DBObject projection, final int numToSkip, final int batchSize) {
        return new DBCursor(this, query, projection, getReadPreference()).batchSize(batchSize).skip(numToSkip);
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query the selection criteria using query operators. Omit the query parameter or pass an empty document to return all
     *              documents in the collection.
     * @return A cursor to the documents that match the query criteria
     */
    @Override
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

        final Find find = new Find()
                .select(toFieldSelectorDocument(projection))
                .where(toDocument(query))
                .order(toDocument(sort))
                .readPreference(readPreference.toNew())
                .batchSize(-1);

        try {
            MongoCursor<DBObject> cursor = new QueryOperation<DBObject>(getNamespace(), find, documentCodec, objectCodec, getBufferPool(),
                    getSession(), false).execute();

            return cursor.hasNext() ? cursor.next() : null;
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
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
     * Please extend DBCollection and ovverride {@link #doapply(DBObject)} if you need to add specific fields before saving object to
     * collection.
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
     * Please extend DBCollection and ovverride {@link #doapply(DBObject)} if you need to add specific fields before saving object to
     * collection.
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
        Find find = new Find(toDocument(query)).limit((int) limit).skip((int) skip).readPreference(readPreference.toNew());

        return executeOperation(new CountOperation(find, getNamespace(), getDocumentCodec(), getBufferPool(), getSession(), false));
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
            new CommandOperation("admin", renameCommand, getDocumentCodec(), getDB().getClusterDescription(), getBufferPool(),
                    getSession(), false).execute();
            return getDB().getCollection(newName);
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
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
        return group(new GroupCommand(this, key, cond, initial, reduce, finalize), readPreference);
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
        return group(cmd.toNew());
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * Deprecated. Use {@link #group(com.mongodb.GroupCommand)} instead.
     *
     * @param args specifies the arguments to the group function
     * @return a document with the grouped records as well as the command meta-data
     */
    @Override
    @Deprecated
    public DBObject group(final DBObject args) {
        final Document commandDocument = new Document("group", toDocument(args).append("ns", getName()));
        return group(new Command(commandDocument));
    }


    private DBObject group(final Command command) {
        final GroupCommandResult commandResult = new GroupCommandResult(getDB().executeCommand(command));
        return toDBList(commandResult.getValue());
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
        final Find find = new Find()
                .filter(toDocument(query))
                .readPreference(readPreference.toNew());
        final Distinct distinctOperation = new Distinct(getName(), fieldName, find);
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

        final MapReduceCommandResultCodec<DBObject> mapReduceCodec =
                new MapReduceCommandResultCodec<DBObject>(getPrimitiveCodecs(), objectCodec);

        final org.mongodb.CommandResult executionResult;

        try {
            executionResult = new CommandOperation(getDB().getName(),
                    command.toNew(),
                    mapReduceCodec,
                    getDB().getClusterDescription(),
                    getBufferPool(), getSession(), false).execute();
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }

        return command.getOutputType() == MapReduceCommand.OutputType.INLINE
                ? new MapReduceOutput(this, new MapReduceInlineCommandResult<DBObject>(executionResult))
                : new MapReduceOutput(this, new MapReduceCommandResult(executionResult));
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
     * @param unique specify true to create a unique index so that the collection will not accept insertion of documents where the index
     *               key or keys matches an existing value in the index
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
        final Insert<Document> insertIndexOperation
                = new Insert<Document>(org.mongodb.WriteConcern.ACKNOWLEDGED, toIndexDetailsDocument(keys, options));
        insertIndex(insertIndexOperation, documentCodec);
    }

    /**
     * Creates an ascending index on the field specified with default options, if that index does not already exist.
     *
     * @param name specifies name of field to index on
     */
    @Override
    public void ensureIndex(final String name) {
        final Index index = Index.builder().addKey(new Index.OrderedKey(name, OrderBy.ASC)).build();
        final Document indexDetails = index.toDocument();
        indexDetails.append(NAMESPACE_KEY_NAME, getNamespace().getFullName());
        final Insert<Document> insertIndexOperation = new Insert<Document>(org.mongodb.WriteConcern.ACKNOWLEDGED, indexDetails);
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
     * @param keys      a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options   a document that controls the creation of the index.
     * @param dbEncoder specifies the encoder that used during operation
     */
    @Override
    public void createIndex(final DBObject keys, final DBObject options, final DBEncoder dbEncoder) {

        final Encoder<DBObject> encoder = toEncoder(dbEncoder);
        final Document indexDetails = toIndexDetailsDocument(keys, options);

        final Insert<DBObject> insertIndexOperation = new Insert<DBObject>(org.mongodb.WriteConcern.ACKNOWLEDGED, toDBObject(indexDetails));
        insertIndex(insertIndexOperation, encoder);
    }

    private <T> void insertIndex(final Insert<T> insertIndexOperation, final Encoder<T> encoder) {
        try {
            new InsertOperation<T>(new MongoNamespace(getDB().getName(), "system.indexes"), insertIndexOperation, encoder,
                    getBufferPool(), getSession(), false).execute();
        } catch (org.mongodb.MongoException exception) {
            throw mapException(exception);
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
        this.hintFields = indexes;
    }

    /**
     * Get hint fields for this collection (used to optimize queries).
     * @return a list of {@code DBObject} to be used as hints.
     */
    public List<DBObject> getHintFields() {
        return hintFields;
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
        final Command mongoCommand;

        if (remove) {
            final FindAndRemove<DBObject> mongoFindAndRemove = new FindAndRemove<DBObject>()
                    .where(toNullableDocument(query))
                    .sortBy(toNullableDocument(sort))
                    .returnNew(returnNew);
            mongoCommand = new org.mongodb.command.FindAndRemove(mongoFindAndRemove, getName());
        }
        else {
            if (update == null) {
                throw new IllegalArgumentException("Update document can't be null");
            }
            if (!update.keySet().isEmpty() && update.keySet().iterator().next().charAt(0) == '$') {

                final FindAndUpdate<DBObject> mongoFindAndUpdate = new FindAndUpdate<DBObject>()
                        .where(toNullableDocument(query))
                        .sortBy(toNullableDocument(sort))
                        .returnNew(returnNew)
                        .select(toFieldSelectorDocument(fields))
                        .updateWith(toUpdateOperationsDocument(update))
                        .upsert(upsert);
                mongoCommand = new org.mongodb.command.FindAndUpdate<DBObject>(mongoFindAndUpdate, getName());
            }
            else {
                final FindAndReplace<DBObject> mongoFindAndReplace
                        = new FindAndReplace<DBObject>(update)
                        .where(toNullableDocument(query))
                        .sortBy(toNullableDocument(sort))
                        .select(toFieldSelectorDocument(fields))
                        .returnNew(returnNew)
                        .upsert(upsert);
                mongoCommand = new org.mongodb.command.FindAndReplace<DBObject>(mongoFindAndReplace, getName());
            }
        }

        final FindAndModifyCommandResultCodec<DBObject> findAndModifyCommandResultCodec =
                new FindAndModifyCommandResultCodec<DBObject>(getPrimitiveCodecs(), objectCodec);

        final FindAndModifyCommandResult<DBObject> commandResult;
        try {
            final org.mongodb.CommandResult executionResult = new CommandOperation(getDB().getName(), mongoCommand,
                    findAndModifyCommandResultCodec, getDB().getClusterDescription(), getBufferPool(), getSession(), false).execute();
            commandResult = new FindAndModifyCommandResult<DBObject>(executionResult);
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }

        return commandResult.getValue();
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
            getDB().executeCommand(new Drop(getName()));
        } catch (CommandFailureException ex) {
            if (!"ns not found".equals(ex.getCommandResult().getErrorMessage())) {
                throw ex;
            }
            //otherwise ignore this, as dropping a DB that doesn't exist is fine
        }
    }

    @Override
    public synchronized DBDecoderFactory getDBDecoderFactory() {
        return decoderFactory;
    }

    @Override
    public synchronized void setDBDecoderFactory(final DBDecoderFactory factory) {
        this.decoderFactory = factory;

        //Are we are using default factory?
        // If yes then we can use CollectibleDBObjectCodec directly, otherwise it will be wrapped.
        final Decoder<DBObject> decoder = (factory == null || factory == DefaultDBDecoder.FACTORY)
                ? createCollectibleDBObjectCodec()
                : new DBDecoderAdapter(factory.create(), this, getBufferPool());
        this.objectCodec = new CompoundDBObjectCodec(objectCodec.getEncoder(), decoder);
    }

    @Override
    public synchronized DBEncoderFactory getDBEncoderFactory() {
        return this.encoderFactory;
    }

    @Override
    public synchronized void setDBEncoderFactory(final DBEncoderFactory factory) {
        this.encoderFactory = factory;

        //Are we are using default factory?
        // If yes then we can use CollectibleDBObjectCodec directly, otherwise it will be wrapped.
        final Encoder<DBObject> encoder = (factory == null || factory == DefaultDBEncoder.FACTORY)
                ? createCollectibleDBObjectCodec()
                : new DBEncoderFactoryAdapter(encoderFactory);
        this.objectCodec = new CompoundDBObjectCodec(encoder, objectCodec.getDecoder());
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

        final Find queryForCollectionNamespace = new Find(
                new Document(NAMESPACE_KEY_NAME, getNamespace().getFullName()))
                .readPreference(org.mongodb.ReadPreference.primary());

        try {
            MongoCursor<Document> cursor =
                    new QueryOperation<Document>(new MongoNamespace(database.getName(), "system.indexes"), queryForCollectionNamespace,
                            documentCodec, documentCodec, getBufferPool(), getSession(), false).execute();

            while (cursor.hasNext()) {
                res.add(DBObjects.toDBObject(cursor.next()));
            }
            return res;
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
    }

    @Override
    public void dropIndex(final DBObject keys) {
        final List<Index.Key<?>> keysFromDBObject = getKeysFromDBObject(keys);
        final Index indexToDrop = Index.builder().addKeys(keysFromDBObject).build();
        final DropIndex dropIndex = new DropIndex(getName(), indexToDrop.getName());
        getDB().executeCommand(dropIndex);
    }

    @Override
    public void dropIndex(final String name) {
        final DropIndex dropIndex = new DropIndex(getName(), name);
        getDB().executeCommand(dropIndex);
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
        final org.mongodb.CommandResult commandResult = getDB().executeCommand(new CollStats(getName()));
        return new CommandResult(commandResult);
    }

    @Override
    public boolean isCapped() {
        final CommandResult commandResult = getStats();
        final Object cappedField = commandResult.get("capped");
        return cappedField != null && (cappedField.equals(1) || cappedField.equals(true));
    }

    @Override
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
        setObjectFactory(objectFactory.update(aClass, Arrays.asList(path.split("\\."))));
    }

    /**
     * Sets the internal class for the given path in the document hierarchy
     *
     * @param path the path to map the given Class to
     * @return the class for a given path in the hierarchy
     */
    protected Class<? extends DBObject> getInternalClass(final String path) {
        return objectFactory.getClassForPath(Arrays.asList(path.split("\\.")));
    }

    @Override
    public String toString() {
        return "DBCollection{" +
                "database=" + database +
                ", name='" + name + '\'' +
                '}';
    }

    synchronized DBObjectFactory getObjectFactory() {
        return this.objectFactory;
    }

    synchronized void setObjectFactory(final DBObjectFactory factory) {
        this.objectFactory = factory;
        this.objectCodec = new CompoundDBObjectCodec(objectCodec.getEncoder(), createCollectibleDBObjectCodec());
    }

    private CollectibleDBObjectCodec createCollectibleDBObjectCodec() {
        return new CollectibleDBObjectCodec(
                getDB(),
                getPrimitiveCodecs(),
                new ObjectIdGenerator(),
                getObjectFactory()
        );
    }

    private PrimitiveCodecs getPrimitiveCodecs() {
        return PrimitiveCodecs.createDefault();
    }

    private Document toIndexDetailsDocument(final DBObject keys, final DBObject options) {
        String indexName = null;
        boolean unique = false;
        boolean dropDups = false;
        boolean sparse = false;
        boolean background = false;
        int expireAfterSeconds = -1;

        final Index.Builder builder = Index.builder();
        if (options != null) {
            final DBObject optionsCopy = new BasicDBObject(options.toMap());
            indexName = (String) optionsCopy.get("name");
            unique = FieldHelpers.asBoolean(optionsCopy.removeField("unique"));
            dropDups = FieldHelpers.asBoolean(optionsCopy.removeField("dropDups"));
            sparse = FieldHelpers.asBoolean(optionsCopy.removeField("sparse"));
            background = FieldHelpers.asBoolean(optionsCopy.removeField("background"));
            if (options.get("expireAfterSeconds") != null) {
                expireAfterSeconds = Integer.parseInt(optionsCopy.removeField("expireAfterSeconds").toString());
            }
            for (final String extraKey : optionsCopy.keySet()) {
                builder.extra(extraKey, optionsCopy.get(extraKey));
            }
        }

        builder
            .name(indexName)
            .unique(unique)
            .dropDups(dropDups)
            .sparse(sparse)
            .background(background)
            .expireAfterSeconds(expireAfterSeconds)
            .addKeys(getKeysFromDBObject(keys));

        final Document indexDetails = builder.build().toDocument();
        indexDetails.append(NAMESPACE_KEY_NAME, getNamespace().getFullName());
        return indexDetails;
    }

    private List<Index.Key<?>> getKeysFromDBObject(final DBObject fields) {
        final List<Index.Key<?>> keys = new ArrayList<Index.Key<?>>();
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

    public Session getSession() {
        return getDB().getSession();
    }

    CollectibleCodec<DBObject> getObjectCodec() {
        return objectCodec;
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

    BufferProvider getBufferPool() {
        return getDB().getBufferPool();
    }

    private <T> T executeOperation(final Operation<T> operation) {
        try {
            return operation.execute();
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
    }
}