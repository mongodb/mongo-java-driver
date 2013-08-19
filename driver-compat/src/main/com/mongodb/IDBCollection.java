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

import java.util.List;

/**
 * Interface extracted from the old DBCollection.  The aim of this interface is simply to keep track of what needs to be
 * implemented for backwards compatibility.
 */
@SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
public interface IDBCollection {
    WriteResult insert(DBObject[] arr, WriteConcern concern);

    WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder);

    WriteResult insert(DBObject o, WriteConcern concern);

    WriteResult insert(DBObject... arr);

    WriteResult insert(WriteConcern concern, DBObject... arr);

    WriteResult insert(List<DBObject> list);

    WriteResult insert(List<DBObject> list, WriteConcern concern);

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
    WriteResult insert(List<DBObject> documents, WriteConcern aWriteConcern, DBEncoder dbEncoder);

    WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern);

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
    WriteResult update(DBObject query, DBObject update, boolean upsert, boolean multi, WriteConcern aWriteConcern, DBEncoder encoder);

    WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi);

    WriteResult update(DBObject q, DBObject o);

    WriteResult updateMulti(DBObject q, DBObject o);

    WriteResult remove(DBObject o, WriteConcern concern);

    /**
     * Remove documents from a collection.
     *
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                     documents in the collection.
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param encoder      {@code DBEncoder} to be used
     * @return the result of the operation
     */
    WriteResult remove(DBObject query, WriteConcern aWriteConcern, DBEncoder encoder);

    /**
     * Remove documents from a collection.
     *
     * @param query he deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents in the collection.
     * @return the result of the operation
     */
    WriteResult remove(DBObject query);

    @Deprecated
    DBCursor find(DBObject query, DBObject fields, int numToSkip, int batchSize, int options);

    @Deprecated
    DBCursor find(DBObject query, DBObject fields, int numToSkip, int batchSize);

    DBObject findOne(Object obj);

    DBObject findOne(Object obj, DBObject fields);

    DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update,
                           boolean returnNew, boolean upsert);

    DBObject findAndModify(DBObject query, DBObject sort, DBObject update);

    DBObject findAndModify(DBObject query, DBObject update);

    DBObject findAndRemove(DBObject query);

    void createIndex(DBObject keys);

    void createIndex(DBObject keys, DBObject options);

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys
     * @param options
     * @param encoder the DBEncoder to use
     * @throws com.mongodb.MongoException
     */
    void createIndex(DBObject keys, DBObject options, DBEncoder encoder);

    void ensureIndex(String name);

    void ensureIndex(DBObject keys);

    void ensureIndex(DBObject keys, String name);

    void ensureIndex(DBObject keys, String name, boolean unique);

    void ensureIndex(DBObject keys, DBObject optionsIN);

    void resetIndexCache();

    void setHintFields(List<DBObject> lst);

    DBCursor find(DBObject ref);

    DBCursor find(DBObject ref, DBObject keys);

    DBCursor find();

    DBObject findOne();

    DBObject findOne(DBObject o);

    DBObject findOne(DBObject o, DBObject fields);

    DBObject findOne(DBObject o, DBObject fields, DBObject orderBy);

    DBObject findOne(DBObject o, DBObject fields, ReadPreference readPref);

    DBObject findOne(DBObject o, DBObject fields, DBObject orderBy, ReadPreference readPref);

    Object apply(DBObject o);

    Object apply(DBObject jo, boolean ensureID);

    WriteResult save(DBObject jo);

    WriteResult save(DBObject jo, WriteConcern concern);

    void dropIndexes();

    void dropIndexes(String name);

    void drop();

    long count();

    long count(DBObject query);

    long count(DBObject query, ReadPreference readPrefs);

    long getCount();

    long getCount(ReadPreference readPrefs);

    long getCount(DBObject query);

    long getCount(DBObject query, DBObject fields);

    long getCount(DBObject query, DBObject fields, ReadPreference readPrefs);

    long getCount(DBObject query, DBObject fields, long limit, long skip);

    long getCount(DBObject query, DBObject fields, long limit, long skip, ReadPreference readPrefs);

    DBCollection rename(String newName);

    DBCollection rename(String newName, boolean dropTarget);

    DBObject group(DBObject key, DBObject cond, DBObject initial, String reduce);

    DBObject group(DBObject key, DBObject cond, DBObject initial, String reduce, String finalize);

    DBObject group(DBObject key, DBObject cond, DBObject initial, String reduce, String finalize,
                   ReadPreference readPrefs);

    DBObject group(GroupCommand cmd);

    DBObject group(GroupCommand cmd, ReadPreference readPrefs);

    @Deprecated
    DBObject group(DBObject args);

    List distinct(String key);

    List distinct(String key, ReadPreference readPrefs);

    List distinct(String key, DBObject query);

    List distinct(String key, DBObject query, ReadPreference readPrefs);

    MapReduceOutput mapReduce(String map, String reduce, String outputTarget, DBObject query);

    MapReduceOutput mapReduce(String map, String reduce, String outputTarget, MapReduceCommand.OutputType outputType,
                              DBObject query);

    MapReduceOutput mapReduce(String map, String reduce, String outputTarget, MapReduceCommand.OutputType outputType,
                              DBObject query, ReadPreference readPrefs);

    MapReduceOutput mapReduce(MapReduceCommand command);

    MapReduceOutput mapReduce(DBObject command);

    AggregationOutput aggregate(DBObject firstOp, DBObject... additionalOps);

    List<DBObject> getIndexInfo();

    void dropIndex(DBObject keys);

    void dropIndex(String name);

    CommandResult getStats();

    boolean isCapped();

    DBCollection getCollection(String n);

    String getName();

    String getFullName();

    DB getDB();

    @Override
    int hashCode();

    @Override
    boolean equals(Object o);

    @Override
    String toString();

    Class getObjectClass();

    void setWriteConcern(WriteConcern concern);

    WriteConcern getWriteConcern();

    void setReadPreference(ReadPreference preference);

    ReadPreference getReadPreference();

    @Deprecated
    void slaveOk();

    void addOption(int option);

    void setOptions(int options);

    void resetOptions();

    int getOptions();

    void setDBDecoderFactory(DBDecoderFactory fact);

    DBDecoderFactory getDBDecoderFactory();

    void setDBEncoderFactory(DBEncoderFactory fact);

    DBEncoderFactory getDBEncoderFactory();

    //These are all the classes used by the old DBCollection, that have not yet been implemented in driver-compat.
}
