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

import org.mongodb.MongoCollection;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.InsertResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;

import java.util.Arrays;
import java.util.List;

public class DBCollection {
    private final MongoCollection<DBObject> collection;
    private final DB database;
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    DBCollection(final MongoCollection<DBObject> collection, final DB database) {
        this.collection = collection;
        this.database = database;
    }


    public WriteResult insert(final DBObject document, final WriteConcern writeConcern) {
        return insert(Arrays.asList(document), writeConcern);
    }

    public WriteResult insert(final DBObject... documents) {
        return insert(Arrays.asList(documents), getWriteConcern());
    }

    public WriteResult insert(final WriteConcern writeConcernToUse, final DBObject... documents) {
        return insert(documents, writeConcernToUse);
    }

    public WriteResult insert(final DBObject[] documents, final WriteConcern writeConcern) {
        return insert(Arrays.asList(documents), writeConcern);
    }

    public WriteResult insert(final List<DBObject> documents) {
        return insert(documents, getWriteConcern());
    }

    public WriteResult insert(final List<DBObject> documents, final WriteConcern writeConcern) {
        final MongoInsert<DBObject> insert = new MongoInsert<DBObject>(documents).writeConcern(writeConcern.toNew());
        final InsertResult result = collection.insert(insert);
        return new WriteResult(result, writeConcern.toNew());
    }

    /**
     * Performs an update operation.
     *
     * @param q       search query for old object to update
     * @param o       object with which to update <tt>q</tt>
     * @param upsert  if the database should create the element if it does not exist
     * @param multi   if the update should be applied to all objects matching (db version 1.1.3 and above). An object
     *                will not be inserted if it does not exist in the collection and upsert=true and multi=true. See <a
     *                href="http://www.mongodb.org/display/DOCS/Atomic+Operations">http://www.mongodb.org/display/DOCS/Atomic+Operations</a>
     * @param concern the write concern
     * @return
     * @throws MongoException
     * @dochub update
     */
    public WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern) {
        if (o == null) {
            throw new IllegalArgumentException("update can not be null");
        }

        if (q == null) {
            throw new IllegalArgumentException("update query can not be null");
        }

        final UpdateResult result;

        if (!o.keySet().isEmpty() && o.keySet().iterator().next().startsWith("$")) {
            MongoUpdate update = new MongoUpdate(DBObjects.toQueryFilterDocument(q),
                                                 DBObjects.toUpdateOperationsDocument(o));
            update.isMulti(multi).isUpsert(upsert).writeConcern(concern.toNew());
            result = collection.update(update);
        }
        else {
            MongoReplace<DBObject> replace = new MongoReplace<DBObject>(DBObjects.toQueryFilterDocument(q), o);
            replace.isUpsert(upsert);
            result = collection.replace(replace);
        }
        return new WriteResult(result, concern.toNew());

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
    public WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi) {
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
    public WriteResult update(DBObject q, DBObject o) {
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
    public WriteResult updateMulti(DBObject q, DBObject o) {
        return update(q, o, false, true);
    }


    public WriteResult remove(final DBObject filter, final WriteConcern writeConcernToUse) {
        final MongoRemove remove = new MongoRemove(DBObjects.toQueryFilterDocument(filter));
        final RemoveResult result = collection.remove(remove);
        return new WriteResult(result, writeConcernToUse.toNew());
    }

    public DBCursor find(final DBObject filter, final DBObject fields) {
        return new DBCursor(collection, new MongoFind().
                where(DBObjects.toQueryFilterDocument(filter)).
                select(DBObjects.toFieldSelectorDocument(fields)).
                readPreference(getReadPreference().toNew()));
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
    public DBObject findOne(DBObject o) {
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
    public DBObject findOne(DBObject o, DBObject fields) {
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
    public DBObject findOne(DBObject o, DBObject fields, DBObject orderBy) {
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
    public DBObject findOne(DBObject o, DBObject fields, ReadPreference readPref) {
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
    public DBObject findOne(DBObject o, DBObject fields, DBObject orderBy, ReadPreference readPref) {

        MongoFind find = new MongoFind(DBObjects.toQueryFilterDocument(o)).select(
                DBObjects.toFieldSelectorDocument(fields));
        find.readPreference(readPref.toNew());

        DBObject obj = collection.findOne(find);

        if (obj != null && (fields != null && fields.keySet().size() > 0)) {
            obj.markAsPartialObject();
        }
        return obj;
    }

    /**
     * Finds an object by its id. This compares the passed in value to the _id field of the document
     *
     * @param obj any valid object
     * @return the object, if found, otherwise <code>null</code>
     * @throws MongoException
     */
    public DBObject findOne(Object obj) {
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
    public DBObject findOne(Object obj, DBObject fields) {
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
    public long count(DBObject query) {
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
    public long count(DBObject query, ReadPreference readPrefs) {
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
    public long getCount(ReadPreference readPrefs) {
        return getCount(new BasicDBObject(), null, readPrefs);
    }

    /**
     * calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject)} with null fields.
     *
     * @param query query to match
     * @return
     * @throws MongoException
     */
    public long getCount(DBObject query) {
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
    public long getCount(DBObject query, DBObject fields) {
        return getCount(query, fields, 0, 0);
    }

    /**
     * calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, long, long,
     * com.mongodb.ReadPreference)} with limit=0 and skip=0
     *
     * @param query     query to match
     * @param fields    fields to return
     * @param readPrefs ReadPreferences for this command
     * @return
     * @throws MongoException
     */
    public long getCount(DBObject query, DBObject fields, ReadPreference readPrefs) {
        return getCount(query, fields, 0, 0, readPrefs);
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
    public long getCount(DBObject query, DBObject fields, long limit, long skip) {
        return getCount(query, fields, limit, skip, getReadPreference());
    }

    /**
     * Returns the number of documents in the collection that match the specified query
     *
     * @param query     query to select documents to count
     * @param fields    fields to return. This is ignored.
     * @param limit     limit the count to this value
     * @param skip      number of entries to skip
     * @param readPrefs ReadPreferences for this command
     * @return number of documents that match query and fields
     * @throws MongoException
     */

    public long getCount(DBObject query, DBObject fields, long limit, long skip, ReadPreference readPrefs) {
        MongoFind find = new MongoFind(DBObjects.toQueryFilterDocument(query));
        find.limit(limit).skip((int) skip).readPreference(
                readPrefs.toNew());   // TODO: investigate case of int to long for skip
        return collection.count(find);
    }

    public ReadPreference getReadPreference() {
        return readPreference != null ? readPreference : database.getReadPreference();
    }

    public WriteConcern getWriteConcern() {
        return writeConcern != null ? writeConcern : database.getWriteConcern();
    }
}
