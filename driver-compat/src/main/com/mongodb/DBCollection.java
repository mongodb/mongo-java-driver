/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import com.mongodb.serializers.CollectibleDBObjectSerializer;
import org.bson.types.Document;
import org.mongodb.Get;
import org.mongodb.Index;
import org.mongodb.MongoCollection;
import org.mongodb.MongoStream;
import org.mongodb.MongoWritableStream;
import org.mongodb.OrderBy;
import org.mongodb.UpdateOperationsDocument;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.command.MongoDuplicateKeyException;
import org.mongodb.result.InsertResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.serializers.ObjectIdGenerator;
import org.mongodb.util.FieldHelpers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ThreadSafe
@SuppressWarnings({ "rawtypes", "deprecation" })
public class DBCollection implements IDBCollection {
    private volatile MongoCollection<DBObject> collection;
    private final DB database;
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;
    private Class<? extends DBObject> objectClass = BasicDBObject.class;
    private final Map<String, Class<? extends DBObject>> pathToClassMap = new HashMap<String,
                                                                                     Class<? extends DBObject>>();
    //    private ReflectionDBObject.JavaWrapper _wrapper = null;

    DBCollection(final String name, final DB database) {
        this.database = database;
        setCollection(name);
    }

    public WriteResult insert(final DBObject document, final WriteConcern writeConcern) {
        return insert(Arrays.asList(document), writeConcern);
    }

    public WriteResult insert(final DBObject... documents) {
        return insert(Arrays.asList(documents), getWriteConcern());
    }

    public WriteResult insert(final WriteConcern writeConcern, final DBObject... documents) {
        return insert(documents, writeConcern);
    }

    public WriteResult insert(final DBObject[] documents, final WriteConcern writeConcern) {
        return insert(Arrays.asList(documents), writeConcern);
    }

    @Override
    public WriteResult insert(final DBObject[] arr, final WriteConcern concern, final DBEncoder encoder) {
        throw new UnsupportedOperationException();
    }

    public WriteResult insert(final List<DBObject> documents) {
        return insert(documents, getWriteConcern());
    }

    public WriteResult insert(final List<DBObject> documents, final WriteConcern writeConcern) {
        try {
            final InsertResult result = collection.writeConcern(writeConcern.toNew()).insert(documents);
            return new WriteResult(result, writeConcern);
        } catch (MongoDuplicateKeyException e) {
            throw new MongoException.DuplicateKey(e);
        }
    }

    @Override
    public WriteResult insert(final List<DBObject> list, final WriteConcern concern, final DBEncoder encoder) {
        throw new UnsupportedOperationException();
    }


    public WriteResult save(final DBObject obj) {
        return save(obj, getWriteConcern());
    }

    public WriteResult save(final DBObject obj, final WriteConcern wc) {
        try {
            final UpdateResult result = collection.writeConcern(wc.toNew()).save(obj);
            return new WriteResult(result, wc);
        } catch (MongoDuplicateKeyException e) {
            throw new MongoException.DuplicateKey(e);
        } catch (org.mongodb.MongoException e) {
            throw new MongoException(e);
        }
    }

    /**
     * Performs an update operation.
     *
     * @param q       search query for old object to update
     * @param o       object with which to update <tt>q</tt>
     * @param upsert  if the database should create the element if it does not exist
     * @param multi   if the update should be applied to all objects matching (db version 1.1.3 and above). An object
     *                will not be inserted if it does not exist in the collection and upsert=true and multi=true. See <a
     *                href="http://www.mongodb.org/display/DOCS/Atomic+Operations">http://www.mongodb
     *                .org/display/DOCS/Atomic+Operations</a>
     * @param concern the write concern
     * @return
     * @throws MongoException
     * @dochub update
     */
    public WriteResult update(final DBObject q, final DBObject o, final boolean upsert, final boolean multi,
                              final WriteConcern concern) {
        if (o == null) {
            throw new IllegalArgumentException("update can not be null");
        }

        if (q == null) {
            throw new IllegalArgumentException("update query can not be null");
        }

        MongoStream<DBObject> stream = collection.filter(DBObjects.toQueryFilterDocument(q));
        if (multi) {
            stream = stream.noLimit();
        }
        final MongoWritableStream<DBObject> writableStream = stream.writeConcern(concern.toNew());
        try {
            final UpdateResult result;
            if (!o.keySet().isEmpty() && o.keySet().iterator().next().startsWith("$")) {
                result = upsert ?
                         writableStream.modifyOrInsert(DBObjects.toUpdateOperationsDocument(o)) :
                         writableStream.modify(DBObjects.toUpdateOperationsDocument(o));
            }
            else {
                result = upsert ?
                         writableStream.replaceOrInsert(o) :
                         writableStream.replace(o);
            }
            return new WriteResult(result, concern);
        } catch (org.mongodb.MongoException e) {
            throw new MongoException(e);
        }
    }

    @Override
    public WriteResult update(final DBObject q, final DBObject o, final boolean upsert, final boolean multi,
                              final WriteConcern concern, final DBEncoder encoder) {
        throw new UnsupportedOperationException();
    }

    /**
     * calls {@link DBCollection#update(com.mongodb.DBObject, com.mongodb.DBObject, boolean, boolean,
     * com.mongodb.WriteConcern)} with default WriteConcern.
     *
     * @param q      search query for old object to update
     * @param o      object with which to update <tt>q</tt>
     * @param upsert if the database should create the element if it does not exist
     * @param multi  if the update should be applied to all objects matching (db version 1.1.3 and above) See
     *               http://www.mongodb.org/display/DOCS/Atomic+Operations
     * @return
     * @throws MongoException
     * @dochub update
     */
    public WriteResult update(final DBObject q, final DBObject o, final boolean upsert, final boolean multi) {
        return update(q, o, upsert, multi, getWriteConcern());
    }

    /**
     * calls {@link DBCollection#update(com.mongodb.DBObject, com.mongodb.DBObject, boolean, boolean)} with upsert=false
     * and multi=false
     *
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @return
     * @throws MongoException
     * @dochub update
     */
    public WriteResult update(final DBObject q, final DBObject o) {
        return update(q, o, false, false);
    }

    /**
     * calls {@link DBCollection#update(com.mongodb.DBObject, com.mongodb.DBObject, boolean, boolean)} with upsert=false
     * and multi=true
     *
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @return
     * @throws MongoException
     * @dochub update
     */
    public WriteResult updateMulti(final DBObject q, final DBObject o) {
        return update(q, o, false, true);
    }

    public WriteResult remove(final DBObject filter) {
        return remove(filter, getWriteConcern());
    }

    @Override
    public DBCursor find(final DBObject query, final DBObject fields, final int numToSkip, final int batchSize,
                         final int options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBCursor find(final DBObject query, final DBObject fields, final int numToSkip, final int batchSize) {
        throw new UnsupportedOperationException();
    }


    public WriteResult remove(final DBObject filter, final WriteConcern writeConcernToUse) {
        final RemoveResult result = collection.filter(DBObjects.toQueryFilterDocument(filter))
                                              .writeConcern(writeConcernToUse.toNew()).remove();
        return new WriteResult(result, writeConcernToUse);
    }

    @Override
    public WriteResult remove(final DBObject o, final WriteConcern concern, final DBEncoder encoder) {
        throw new UnsupportedOperationException();
    }

    public DBCursor find(final DBObject filter) {
        return find(filter, null);
    }

    public DBCursor find(final DBObject filter, final DBObject fields) {
        return new DBCursor(this, filter, fields, getReadPreference());
    }

    /**
     * Queries for all objects in this collection.
     *
     * @return a cursor which will iterate over every object
     * @dochub find
     */
    public DBCursor find() {
        return find(new BasicDBObject(), null);
    }


    /**
     * Returns a single object from this collection.
     *
     * @return the object found, or <code>null</code> if the collection is empty
     * @throws MongoException
     */
    public DBObject findOne() {
        return findOne(new BasicDBObject());
    }

    /**
     * Returns a single object from this collection matching the query.
     *
     * @param o the query object
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     */
    public DBObject findOne(final DBObject o) {
        return findOne(o, null, null, getReadPreference());
    }

    /**
     * Returns a single object from this collection matching the query.
     *
     * @param o      the query object
     * @param fields fields to return
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne(final DBObject o, final DBObject fields) {
        return findOne(o, fields, null, getReadPreference());
    }

    /**
     * Returns a single obejct from this collection matching the query.
     *
     * @param o       the query object
     * @param fields  fields to return
     * @param orderBy fields to order by
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne(final DBObject o, final DBObject fields, final DBObject orderBy) {
        return findOne(o, fields, orderBy, getReadPreference());
    }

    /**
     * Returns a single object from this collection matching the query.
     *
     * @param o        the query object
     * @param fields   fields to return
     * @param readPref
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne(final DBObject o, final DBObject fields, final ReadPreference readPref) {
        return findOne(o, fields, null, readPref);
    }

    /**
     * Returns a single object from this collection matching the query.
     *
     * @param o       the query object
     * @param fields  fields to return
     * @param orderBy fields to order by
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne(final DBObject o, final DBObject fields, final DBObject orderBy,
                            final ReadPreference readPref) {

        final DBObject obj = collection.filter(DBObjects.toQueryFilterDocument(o))
                                       .sort(DBObjects.toSortCriteriaDocument(orderBy))
                                       .select(DBObjects.toFieldSelectorDocument(fields))
                                       .readPreference(readPref.toNew()).one();

        if (obj != null && (fields != null && fields.keySet().size() > 0)) {
            obj.markAsPartialObject();
        }
        return obj;
    }

    @Override
    public Object apply(final DBObject o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object apply(final DBObject jo, final boolean ensureID) {
        throw new UnsupportedOperationException();
    }

    /**
     * Finds an object by its id. This compares the passed in value to the _id field of the document
     *
     * @param obj any valid object
     * @return the object, if found, otherwise <code>null</code>
     * @throws MongoException
     */
    public DBObject findOne(final Object obj) {
        return findOne(obj, null);
    }


    /**
     * Finds an object by its id. This compares the passed in value to the _id field of the document
     *
     * @param obj    any valid object
     * @param fields fields to return
     * @return the object, if found, otherwise <code>null</code>
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne(final Object obj, final DBObject fields) {
        return findOne(new BasicDBObject("_id", obj), fields);
    }


    /**
     * returns the number of documents in this collection.
     *
     * @return
     * @throws MongoException
     */
    public long count() {
        return getCount(new BasicDBObject(), null);
    }

    /**
     * returns the number of documents that match a query.
     *
     * @param query query to match
     * @return
     * @throws MongoException
     */
    public long count(final DBObject query) {
        return getCount(query, null);
    }

    /**
     * returns the number of documents that match a query.
     *
     * @param query     query to match
     * @param readPrefs ReadPreferences for this query
     * @return
     * @throws MongoException
     */
    public long count(final DBObject query, final ReadPreference readPrefs) {
        return getCount(query, null, readPrefs);
    }


    /**
     * calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject)} with an empty query and null
     * fields.
     *
     * @return number of documents that match query
     * @throws MongoException
     */
    public long getCount() {
        return getCount(new BasicDBObject(), null);
    }

    /**
     * calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.ReadPreference)} with
     * empty query and null fields.
     *
     * @param readPrefs ReadPreferences for this command
     * @return number of documents that match query
     * @throws MongoException
     */
    public long getCount(final ReadPreference readPrefs) {
        return getCount(new BasicDBObject(), null, readPrefs);
    }

    /**
     * calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject)} with null fields.
     *
     * @param query query to match
     * @return
     * @throws MongoException
     */
    public long getCount(final DBObject query) {
        return getCount(query, null);
    }


    /**
     * calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, long, long)} with limit=0 and
     * skip=0
     *
     * @param query  query to match
     * @param fields fields to return
     * @return
     * @throws MongoException
     */
    public long getCount(final DBObject query, final DBObject fields) {
        return getCount(query, fields, 0, 0);
    }

    /**
     * calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, long, long,
     * com.mongodb.ReadPreference)} with limit=0 and skip=0
     *
     * @param query          query to match
     * @param fields         fields to return
     * @param readPreference ReadPreferences for this command
     * @return
     * @throws MongoException
     */
    public long getCount(final DBObject query, final DBObject fields, final ReadPreference readPreference) {
        return getCount(query, fields, 0, 0, readPreference);
    }

    /**
     * calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, long, long,
     * com.mongodb.ReadPreference)} with the DBCollection's ReadPreference
     *
     * @param query  query to match
     * @param fields fields to return
     * @param limit  limit the count to this value
     * @param skip   skip number of entries to skip
     * @return
     * @throws MongoException
     */
    public long getCount(final DBObject query, final DBObject fields, final long limit, final long skip) {
        return getCount(query, fields, limit, skip, getReadPreference());
    }

    /**
     * Returns the number of documents in the collection that match the specified query
     *
     * @param query          query to select documents to count
     * @param fields         fields to return. This is ignored.
     * @param limit          limit the count to this value
     * @param skip           number of entries to skip
     * @param readPreference ReadPreferences for this command
     * @return number of documents that match query and fields
     * @throws MongoException
     */

    public long getCount(final DBObject query, final DBObject fields, final long limit, final long skip,
                         final ReadPreference readPreference) {
        if (limit > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("limit is too large: " + limit);
        }

        if (skip > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("skip is too large: " + skip);
        }
        MongoStream<DBObject> stream = collection;
        if (query != null) {
            stream = stream.filter(DBObjects.toQueryFilterDocument(query));
        }
        // TODO: investigate case of int to long for skip
        return stream.limit((int) limit).skip((int) skip).readPreference(readPreference.toNew()).count();
    }

    @Override
    public DBCollection rename(final String newName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBCollection rename(final String newName, final boolean dropTarget) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBObject group(final DBObject key, final DBObject cond, final DBObject initial, final String reduce) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBObject group(final DBObject key, final DBObject cond, final DBObject initial, final String reduce,
                          final String finalize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBObject group(final DBObject key, final DBObject cond, final DBObject initial, final String reduce,
                          final String finalize, final ReadPreference readPrefs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBObject group(final GroupCommand cmd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBObject group(final GroupCommand cmd, final ReadPreference readPrefs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBObject group(final DBObject args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List distinct(final String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List distinct(final String key, final ReadPreference readPrefs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List distinct(final String key, final DBObject query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List distinct(final String key, final DBObject query, final ReadPreference readPrefs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final DBObject query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final MapReduceCommand.OutputType outputType, final DBObject query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final MapReduceCommand.OutputType outputType, final DBObject query,
                                     final ReadPreference readPrefs) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the name of this collection.
     *
     * @return the name of this collection
     */
    public String getName() {
        return collection.getName();
    }

    /**
     * Returns the full name of this collection, with the database name as a prefix.
     *
     * @return the name of this collection
     */
    public String getFullName() {
        return collection.getNamespace().getFullName();
    }


    /**
     * Finds a collection that is prefixed with this collection's name. A typical use of this might be
     * <blockquote><pre>
     *    DBCollection users = mongo.getCollection( "wiki" ).getCollection( "users" );
     * </pre></blockquote>
     * Which is equivalent to
     * <pre><blockquote>
     *   DBCollection users = mongo.getCollection( "wiki.users" );
     * </pre></blockquote>
     *
     * @param n the name of the collection to find
     * @return the matching collection
     */
    public DBCollection getCollection(final String n) {
        return database.getCollection(getName() + "." + n);
    }

    public void ensureIndex(final DBObject fields) {
        ensureIndex(fields, (DBObject) null);
    }

    @Override
    public void ensureIndex(final DBObject keys, final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureIndex(final DBObject keys, final String name, final boolean unique) {
        final BasicDBObject options = new BasicDBObject("name", name);
        options.append("unique", unique);
        ensureIndex(keys, options);
    }

    // TODO: check if these are all the supported options
    public void ensureIndex(final DBObject fields, final DBObject opts) {
        String name = null;
        boolean unique = false;
        if (opts != null) {
            if (opts.get("name") != null) {
                name = (String) opts.get("name");
            }
            if (opts.get("unique") != null) {
                unique = FieldHelpers.asBoolean(opts.get("unique"));
            }
        }
        final List<Index.Key> keys = getKeysFromDBObject(fields);
        try {
            collection.admin().ensureIndex(new Index(name, unique, keys.toArray(new Index.Key[keys.size()])));
        } catch (MongoDuplicateKeyException exception) {
            throw new MongoException.DuplicateKey(exception);
        }
    }

    @Override
    public void resetIndexCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHintFields(final List<DBObject> lst) {
        throw new UnsupportedOperationException();
    }

    /**
     * calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject,
     * boolean, com.mongodb.DBObject, boolean, boolean)} with fields=null, remove=false, returnNew=false, upsert=false
     *
     * @param query
     * @param sort
     * @param update
     * @return the old document
     * @throws MongoException
     */
    public DBObject findAndModify(final DBObject query, final DBObject sort, final DBObject update) {
        return findAndModify(query, null, sort, false, update, false, false);
    }

    /**
     * calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject,
     * boolean, com.mongodb.DBObject, boolean, boolean)} with fields=null, sort=null, remove=false, returnNew=false,
     * upsert=false
     *
     * @param query
     * @param update
     * @return the old document
     * @throws MongoException
     */
    public DBObject findAndModify(final DBObject query, final DBObject update) {
        return findAndModify(query, null, null, false, update, false, false);
    }

    /**
     * calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject,
     * boolean, com.mongodb.DBObject, boolean, boolean)} with fields=null, sort=null, remove=true, returnNew=false,
     * upsert=false
     *
     * @param query
     * @return the removed document
     * @throws MongoException
     */
    public DBObject findAndRemove(final DBObject query) {
        return findAndModify(query, null, null, true, null, false, false);
    }

    @Override
    public void createIndex(final DBObject keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createIndex(final DBObject keys, final DBObject options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createIndex(final DBObject keys, final DBObject options, final DBEncoder encoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureIndex(final String name) {
        throw new UnsupportedOperationException();
    }

    /**
     * Finds the first document in the query and updates it.
     *
     * @param query     query to match
     * @param fields    fields to be returned
     * @param sort      sort to apply before picking first document
     * @param remove    if true, document found will be removed
     * @param update    update to apply
     * @param returnNew if true, the updated document is returned, otherwise the old document is returned (or it would
     *                  be lost forever)
     * @param upsert    do upsert (insert if document not present)
     * @return the document
     * @throws MongoException
     */
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, final DBObject update,
                                  final boolean returnNew, final boolean upsert) {
        final MongoWritableStream<DBObject> stream = collection.filter(DBObjects.toQueryFilterDocument(query))
                                                               .select(DBObjects.toFieldSelectorDocument(fields))
                                                               .sort(DBObjects.toSortCriteriaDocument(sort))
                                                               .writeConcern(getWriteConcern().toNew());
        if (remove) {
            return stream.removeAndGet();
        }
        else {
            if (update == null) {
                throw new IllegalArgumentException("update document can not be null");
            }
            if (!update.keySet().isEmpty() && update.keySet().iterator().next().charAt(0) == '$') {
                final UpdateOperationsDocument updateOperations = DBObjects.toUpdateOperationsDocument(update);
                return upsert ?
                       stream.modifyOrInsertAndGet(updateOperations, asGetOrder(returnNew)) :
                       stream.modifyAndGet(updateOperations, asGetOrder(returnNew));
            }
            else {
                return upsert ?
                       stream.replaceOrInsertAndGet(update, asGetOrder(returnNew)) :
                       stream.replaceAndGet(update, asGetOrder(returnNew));
            }
        }
    }

    private Get asGetOrder(final boolean returnNew) {
        return returnNew ? Get.BeforeChangeApplied : Get.AfterChangeApplied;
    }

    /**
     * Returns the database this collection is a member of.
     *
     * @return this collection's database
     */

    public DB getDB() {
        return database;
    }

    @Override
    public Class getObjectClass() {
        throw new UnsupportedOperationException();
    }

    /**
     * Set the write concern for this collection. Will be used for writes to this collection. Overrides any setting of
     * write concern at the DB level. See the documentation for {@link WriteConcern} for more information.
     *
     * @param writeConcern write concern to use
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * Get the write concern for this collection.
     *
     * @return
     */
    public WriteConcern getWriteConcern() {
        if (writeConcern != null) {
            return writeConcern;
        }
        return database.getWriteConcern();
    }

    /**
     * Sets the read preference for this collection. Will be used as default for reads from this collection; overrides
     * DB & Connection level settings. See the * documentation for {@link ReadPreference} for more information.
     *
     * @param preference Read Preference to use
     */
    public void setReadPreference(final ReadPreference preference) {
        this.readPreference = preference;
    }

    /**
     * Gets the read preference
     *
     * @return
     */
    public ReadPreference getReadPreference() {
        if (readPreference != null) {
            return readPreference;
        }
        return database.getReadPreference();
    }

    @Override
    public void slaveOk() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addOption(final int option) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOptions(final int options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetOptions() {
        throw new UnsupportedOperationException();
    }


    /**
     * Drops (deletes) this collection. Use with care.
     *
     * @throws MongoException
     */
    public void drop() {
        collection.admin().drop();
    }

    /**
     * performs a map reduce operation
     *
     * @param command object representing the parameters
     * @return
     * @throws MongoException
     */
    public MapReduceOutput mapReduce(final MapReduceCommand command) {
        final DBObject cmd = command.toDBObject();
        // if type in inline, then query options like slaveOk is fine
        CommandResult res;
        if (command.getOutputType() == MapReduceCommand.OutputType.INLINE) {
            res = database.command(cmd, getOptions(),
                                  command.getReadPreference() != null ? command.getReadPreference()
                                                                      : getReadPreference());
        }
        else {
            res = database.command(cmd);
        }
        res.throwOnError();
        return new MapReduceOutput(this, cmd, res);
    }

    @Override
    public MapReduceOutput mapReduce(final DBObject command) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregationOutput aggregate(final DBObject firstOp, final DBObject... additionalOps) {
        throw new UnsupportedOperationException();
    }

    public int getOptions() {
        return 0;   // TODO: Support options
    }

    @Override
    public void setDBDecoderFactory(final DBDecoderFactory fact) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBDecoderFactory getDBDecoderFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDBEncoderFactory(final DBEncoderFactory fact) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBEncoderFactory getDBEncoderFactory() {
        throw new UnsupportedOperationException();
    }

    /**
     * Return a list of the indexes for this collection.  Each object in the list is the "info document" from MongoDB
     *
     * @return list of index documents
     * @throws MongoException
     */
    public List<DBObject> getIndexInfo() {
        final ArrayList<DBObject> res = new ArrayList<DBObject>();
        final List<Document> indexes = collection.admin().getIndexes();
        for (final Document curIndex : indexes) {
            res.add(DBObjects.toDBObject(curIndex));
        }
        return res;
    }

    @Override
    public void dropIndex(final DBObject keys) {
        final List<Index.Key> keysFromDBObject = getKeysFromDBObject(keys);
        Index indexToDrop = new Index(keysFromDBObject.toArray(new Index.Key[keysFromDBObject.size()]));
        collection.admin().dropIndex(indexToDrop);
    }

    @Override
    public void dropIndex(final String name) {
        collection.admin().dropIndex(getIndexFromName(name));
    }

    @Override
    public void dropIndexes() {
        collection.admin().dropIndexes();
    }

    @Override
    public void dropIndexes(final String name) {
        dropIndex(name);
    }

    @Override
    public CommandResult getStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCapped() {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets a default class for objects in this collection; null resets the class to nothing.
     *
     * @param clazz the class
     * @throws IllegalArgumentException if <code>c</code> is not a DBObject
     */
    public synchronized void setObjectClass(final Class<? extends DBObject> clazz) {
        objectClass = clazz;
        resetCollection();
    }

    /**
     * Sets the internal class for the given path in the document hierarchy
     *
     * @param path  the path to map the given Class to
     * @param clazz the Class to map the given path to
     */
    public synchronized void setInternalClass(final String path, final Class<? extends DBObject> clazz) {
        pathToClassMap.put(path, clazz);
        resetCollection();
    }

    MongoCollection<DBObject> toNew() {
        return collection;
    }

    private static Index getIndexFromName(final String name) {
        Index.Key key;
        final String keyField = name.substring(0, name.indexOf("_"));
        final String keyType = name.substring(name.indexOf("_") + 1);
        if (keyType.equals("2d")) {
            key = new Index.GeoKey(keyField);
        }
        else {
            key = new Index.OrderedKey(keyField, OrderBy.fromInt(Integer.valueOf(keyType)));
        }
        return new Index(key);
    }

    private List<Index.Key> getKeysFromDBObject(final DBObject fields) {
        final List<Index.Key> keys = new ArrayList<Index.Key>();
        for (final String key : fields.keySet()) {
            final Object keyType = fields.get(key);
            if (keyType instanceof Integer) {
                keys.add(new Index.OrderedKey(key, OrderBy.fromInt((Integer) fields.get(key))));
            }
            else if (keyType.equals("2d")) {
                keys.add(new Index.GeoKey(key));
            }
            else {
                throw new UnsupportedOperationException("Unsupported index type: " + keyType);
            }

        }
        return keys;
    }

    private void resetCollection() {
        setCollection(getName());
    }

    private void setCollection(final String name) {
        final HashMap<String, Class<? extends DBObject>> map
        = new HashMap<String, Class<? extends DBObject>>(pathToClassMap);
        this.collection = database.toNew()
                                  .getCollection(name,
                                                new CollectibleDBObjectSerializer(database,
                                                                                 database.getMongo()
                                                                                         .getNew()
                                                                                         .getOptions()
                                                                                         .getPrimitiveSerializers(),
                                                                                 new ObjectIdGenerator(),
                                                                                 objectClass,
                                                                                 map));
    }
}