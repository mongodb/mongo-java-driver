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

// Mongo
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This class provides a skeleton implementation of a database collection. <p>A typical invocation sequence is thus
 * <pre>
 * {@code
 * MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
 * DB db = mongo.getDB("mydb");
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
 */
@SuppressWarnings("unchecked")
public abstract class DBCollection {

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param arr     {@code DBObject}'s to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @dochub insert Insert
     */
    public WriteResult insert(DBObject[] arr , WriteConcern concern ){
        return insert( arr, concern, getDBEncoder());
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param arr     {@code DBObject}'s to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @param encoder {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @dochub insert Insert
     */
    public WriteResult insert(DBObject[] arr , WriteConcern concern, DBEncoder encoder) {
        return insert(Arrays.asList(arr), concern, encoder);
    }

    /**
     * Insert a document into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param o       {@code DBObject} to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @dochub insert Insert
     */
    public WriteResult insert(DBObject o , WriteConcern concern ){
        return insert( Arrays.asList(o) , concern );
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added. Collection wide {@code WriteConcern} will be used.
     *
     * @param arr {@code DBObject}'s to be inserted
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    public WriteResult insert(DBObject ... arr){
        return insert( arr , getWriteConcern() );
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param arr     {@code DBObject}'s to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    public WriteResult insert(WriteConcern concern, DBObject ... arr){
        return insert( arr, concern );
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param list list of {@code DBObject} to be inserted
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    public WriteResult insert(List<DBObject> list ){
        return insert( list, getWriteConcern() );
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param list    list of {@code DBObject}'s to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    public WriteResult insert(List<DBObject> list, WriteConcern concern ){
        return insert(list, concern, getDBEncoder() );
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param list    a list of {@code DBObject}'s to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @param encoder {@code DBEncoder} to use to serialise the documents
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    public abstract WriteResult insert(List<DBObject> list, WriteConcern concern, DBEncoder encoder);

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     * <p>
     * If the value of the continueOnError property of the given {@code InsertOptions} is true, that value will override the value of the
     * continueOnError property of the given {@code WriteConcern}.  Otherwise, the value of the
     * continueOnError property of the given {@code WriteConcern} will take effect.
     * </p>
     *
     * @param list    a list of {@code DBObject}'s to be inserted
     * @param insertOptions the options to use for the insert
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    public WriteResult insert(List<DBObject> list, InsertOptions insertOptions) {
        WriteConcern writeConcern = insertOptions.getWriteConcern() != null ? insertOptions.getWriteConcern() : getWriteConcern();
        if (insertOptions.isContinueOnError()) {
            writeConcern = writeConcern.continueOnError(true);
        }
        DBEncoder dbEncoder = insertOptions.getDbEncoder() != null ? insertOptions.getDbEncoder() : getDBEncoder();
        return insert(list, writeConcern, dbEncoder);
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link DBCollection#find(DBObject)}.
     *
     * @param q       the selection criteria for the update
     * @param o       the modifications to apply
     * @param upsert  when true, inserts a document if no document matches the update query criteria
     * @param multi   when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    public WriteResult update( DBObject q , DBObject o , boolean upsert , boolean multi , WriteConcern concern ){
        return update( q, o, upsert, multi, concern, getDBEncoder());
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link DBCollection#find(DBObject)}.
     *
     * @param q       the selection criteria for the update
     * @param o       the modifications to apply
     * @param upsert  when true, inserts a document if no document matches the update query criteria
     * @param multi   when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @param concern {@code WriteConcern} to be used during operation
     * @param encoder the DBEncoder to use
     * @return the result of the operation
     * @throws MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    public abstract WriteResult update( DBObject q , DBObject o , boolean upsert , boolean multi , WriteConcern concern, DBEncoder encoder );

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link DBCollection#find(DBObject)}.  Calls {@link DBCollection#update(com.mongodb.DBObject,
     * com.mongodb.DBObject, boolean, boolean, com.mongodb.WriteConcern)} with default WriteConcern.
     *
     * @param q      the selection criteria for the update
     * @param o      the modifications to apply
     * @param upsert when true, inserts a document if no document matches the update query criteria
     * @param multi  when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @return the result of the operation
     * @throws MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    public WriteResult update( DBObject q , DBObject o , boolean upsert , boolean multi ){
        return update( q , o , upsert , multi , getWriteConcern() );
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link DBCollection#find(DBObject)}.  Calls {@link DBCollection#update(com.mongodb.DBObject,
     * com.mongodb.DBObject, boolean, boolean)} with upsert=false and multi=false
     *
     * @param q the selection criteria for the update
     * @param o the modifications to apply
     * @return the result of the operation
     * @throws MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    public WriteResult update( DBObject q , DBObject o ){
        return update( q , o , false , false );
    }

    /**
     * Modify documents in collection. The query parameter employs the same query selectors, as used in
     * {@link DBCollection#find()}.  Calls {@link DBCollection#update(com.mongodb.DBObject,
     * com.mongodb.DBObject, boolean, boolean)} with upsert=false and multi=true
     *
     * @param q the selection criteria for the update
     * @param o the modifications to apply
     * @return the result of the operation
     * @throws MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    public WriteResult updateMulti( DBObject q , DBObject o ){
        return update( q , o , false , true );
    }

    /**
     * Adds any necessary fields to a given object before saving it to the collection.
     * @param o object to which to add the fields
     */
    protected abstract void doapply( DBObject o );

    /**
     * Remove documents from a collection.
     *
     * @param o       the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                documents in the collection.
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException
     * @mongodb.driver.manual tutorial/remove-documents/ Remove
     */
    public WriteResult remove( DBObject o , WriteConcern concern ){
        return remove(  o, concern, getDBEncoder());
    }

    /**
     * Remove documents from a collection.
     *
     * @param o       the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                documents in the collection.
     * @param concern {@code WriteConcern} to be used during operation
     * @param encoder {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws MongoException
     * @mongodb.driver.manual tutorial/remove-documents/ Remove
     */
    public abstract WriteResult remove( DBObject o , WriteConcern concern, DBEncoder encoder );

    /**
     * Remove documents from a collection. Calls {@link DBCollection#remove(com.mongodb.DBObject, com.mongodb.WriteConcern)} with the
     * default WriteConcern
     *
     * @param o the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents in
     *          the collection.
     * @return the result of the operation
     * @throws MongoException
     * @mongodb.driver.manual tutorial/remove-documents/ Remove
     */
    public WriteResult remove( DBObject o ){
        return remove( o , getWriteConcern() );
    }


    /**
     * Finds objects
     */
    abstract QueryResultIterator find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                                      ReadPreference readPref, DBDecoder decoder);

    abstract QueryResultIterator find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                                      ReadPreference readPref, DBDecoder decoder, DBEncoder encoder);


    /**
     * Calls {@link DBCollection#find(com.mongodb.DBObject, com.mongodb.DBObject, int, int)} and applies the query options
     *
     * @param query     query used to search
     * @param fields    the fields of matching objects to return
     * @param numToSkip number of objects to skip
     * @param batchSize the batch size. This option has a complex behavior, see {@link DBCursor#batchSize(int) }
     * @param options   see {@link com.mongodb.Bytes} QUERYOPTION_*
     * @return the cursor
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     * @deprecated use {@link com.mongodb.DBCursor#skip(int)}, {@link com.mongodb.DBCursor#batchSize(int)} and {@link
     * com.mongodb.DBCursor#setOptions(int)} on the {@code DBCursor} returned from {@link com.mongodb.DBCollection#find(DBObject,
     * DBObject)}
     */
    @Deprecated
    public DBCursor find( DBObject query , DBObject fields , int numToSkip , int batchSize , int options ){
    	return find(query, fields, numToSkip, batchSize).addOption(options);
    }


    /**
     * Finds objects from the database that match a query. A DBCursor object is returned, that can be iterated to go through the results.
     *
     * @param query     query used to search
     * @param fields    the fields of matching objects to return
     * @param numToSkip number of objects to skip
     * @param batchSize the batch size. This option has a complex behavior, see {@link DBCursor#batchSize(int) }
     * @return the cursor
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     * @deprecated use {@link com.mongodb.DBCursor#skip(int)} and {@link com.mongodb.DBCursor#batchSize(int)} on the {@code DBCursor}
     * returned from {@link com.mongodb.DBCollection#find(DBObject, DBObject)}
     */
    @Deprecated
    public DBCursor find( DBObject query , DBObject fields , int numToSkip , int batchSize ) {
    	DBCursor cursor = find(query, fields).skip(numToSkip).batchSize(batchSize);
    	return cursor;
    }

    // ------

    /**
     * Finds an object by its id.
     * This compares the passed in value to the _id field of the document
     *
     * @param obj any valid object
     * @return the object, if found, otherwise null
     * @throws MongoException
     */
    public DBObject findOne( Object obj ){
        return findOne(obj, null);
    }


    /**
     * Finds an object by its id.
     * This compares the passed in value to the _id field of the document
     *
     * @param obj any valid object
     * @param fields fields to return
     * @return the object, if found, otherwise null
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( Object obj, DBObject fields ){
        return findOne(new BasicDBObject("_id", obj), fields);
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
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws MongoException
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert){
        return findAndModify(query, fields, sort, remove, update, returnNew, upsert, 0L, MILLISECONDS);
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
     *                    a server version >= 2.6
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.12.0
     */
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final long maxTime, final TimeUnit maxTimeUnit) {
        BasicDBObject cmd = new BasicDBObject( "findandmodify", _name);
        if (query != null && !query.keySet().isEmpty())
            cmd.append( "query", query );
        if (fields != null && !fields.keySet().isEmpty())
            cmd.append( "fields", fields );
        if (sort != null && !sort.keySet().isEmpty())
            cmd.append( "sort", sort );
        if (maxTime > 0) {
            cmd.append("maxTimeMS", MILLISECONDS.convert(maxTime, maxTimeUnit));
        }

        if (remove)
            cmd.append( "remove", remove );
        else {
            if (update != null && !update.keySet().isEmpty()) {
                // if 1st key doesn't start with $, then object will be inserted as is, need to check it
                String key = update.keySet().iterator().next();
                if (key.charAt(0) != '$')
                    _checkObject(update, false, false);
                cmd.append( "update", update );
            }
            if (returnNew)
                cmd.append( "new", returnNew );
            if (upsert)
                cmd.append( "upsert", upsert );
        }

        if (remove && !(update == null || update.keySet().isEmpty() || returnNew))
            throw new MongoException("FindAndModify: Remove cannot be mixed with the Update, or returnNew params!");

        CommandResult res = this._db.command( cmd );
        if (res.ok() || res.getErrorMessage().equals( "No matching object found" )) {
            return replaceWithObjectClass((DBObject) res.get( "value" ));
        }
        res.throwOnError();
        return null;
    }

    /**
     * Doesn't yet handle internal classes properly, so this method only does something if object class is set but
     * no internal classes are set.
     *
     * @param oldObj  the original value from the command result
     * @return replaced object if necessary, or oldObj
     */
    private DBObject replaceWithObjectClass(DBObject oldObj) {
        if (oldObj == null || getObjectClass() == null &  _internalClass.isEmpty()) {
            return oldObj;
        }

        DBObject newObj = instantiateObjectClassInstance();

        for (String key : oldObj.keySet()) {
            newObj.put(key, oldObj.get(key));
        }
        return newObj;
    }

    private DBObject instantiateObjectClassInstance() {
        try {
            return (DBObject) getObjectClass().newInstance();
        } catch (InstantiationException e) {
            throw new MongoInternalException("can't create instance of type " + getObjectClass(), e);
        } catch (IllegalAccessException e) {
            throw new MongoInternalException("can't create instance of type " + getObjectClass(), e);
        }
    }


    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.  Calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean,
     * com.mongodb.DBObject, boolean, boolean)} with fields=null, remove=false, returnNew=false, upsert=false
     *
     * @param query  specifies the selection criteria for the modification
     * @param sort   determines which document the operation will modify if the query selects multiple documents
     * @param update the modifications to apply
     * @return the document as it was before the modifications.
     * @throws MongoException
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndModify( DBObject query , DBObject sort , DBObject update) {
    	return findAndModify( query, null, sort, false, update, false, false);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.  Calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean,
     * com.mongodb.DBObject, boolean, boolean)} with fields=null, sort=null, remove=false, returnNew=false, upsert=false
     *
     * @param query  specifies the selection criteria for the modification
     * @param update the modifications to apply
     * @return the document as it was before the modifications.
     * @throws MongoException
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndModify( DBObject query , DBObject update ){
    	return findAndModify( query, null, null, false, update, false, false );
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.  Calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean,
     * com.mongodb.DBObject, boolean, boolean)} with fields=null, sort=null, remove=true, returnNew=false, upsert=false
     *
     * @param query specifies the selection criteria for the modification
     * @return the document as it was before it was removed
     * @throws MongoException
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    public DBObject findAndRemove( DBObject query ) {
    	return findAndModify( query, null, null, true, null, false, false );
    }

    // --- START INDEX CODE ---

    /**
     * Forces creation of an ascending index on a field with the default options.
     *
     * @param name name of field to index on
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex( final String name ){
        createIndex( new BasicDBObject( name , 1 ) );
    }

    /**
     * Forces creation of an index on a set of fields with the default options, if one does not already exist.
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex( final DBObject keys ){
        createIndex( keys , defaultOptions( keys ) );
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys    a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options a document that controls the creation of the index.
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex( DBObject keys , DBObject options ){
        createIndex( keys, options, getDBEncoder());
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys   a document that contains pairs with the name of the field or fields to index and order of the index
     * @param name   an identifier for the index. If null or empty, the default name will be used.
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex( DBObject keys , String name ){
        createIndex( keys , name,  false);
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys   a document that contains pairs with the name of the field or fields to index and order of the index
     * @param name   an identifier for the index. If null or empty, the default name will be used.
     * @param unique if the index should be unique
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex( DBObject keys , String name , boolean unique ){
        DBObject options = defaultOptions( keys );
        if (name != null && name.length()>0)
            options.put( "name" , name );
        if ( unique )
            options.put( "unique" , Boolean.TRUE );
        createIndex( keys , options );
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys    a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options a document that controls the creation of the index.
     * @param encoder specifies the encoder that used during operation
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link #createIndex(DBObject, com.mongodb.DBObject)} the encoder is not used.
     */
    @Deprecated
    public abstract void createIndex(DBObject keys, DBObject options, DBEncoder encoder);

    /**
     * Creates an ascending index on a field with default options, if one does not already exist.
     *
     * @param name name of field to index on
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link DBCollection#createIndex(String)} instead
     */
    @Deprecated
    public void ensureIndex( final String name ){
        ensureIndex( new BasicDBObject( name , 1 ) );
    }

    /**
     * Calls {@link DBCollection#ensureIndex(com.mongodb.DBObject, com.mongodb.DBObject)} with default options
     * @param keys an object with a key set of the fields desired for the index
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * 
     * @deprecated use {@link DBCollection#createIndex(DBObject)} instead
     */
    @Deprecated
    public void ensureIndex( final DBObject keys ){
        ensureIndex( keys , defaultOptions( keys ) );
    }

    /**
     * Calls {@link DBCollection#ensureIndex(com.mongodb.DBObject, java.lang.String, boolean)} with unique=false
     *
     * @param keys fields to use for index
     * @param name an identifier for the index
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link DBCollection#createIndex(DBObject, DBObject)} instead
     */
    @Deprecated
    public void ensureIndex( DBObject keys , String name ){
        ensureIndex( keys , name , false );
    }

    /**
     * Ensures an index on this collection (that is, the index will be created if it does not exist).
     *
     * @param keys   fields to use for index
     * @param name   an identifier for the index. If null or empty, the default name will be used.
     * @param unique if the index should be unique
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link DBCollection#createIndex(DBObject, String, boolean)} instead
     */
    @Deprecated
    public void ensureIndex( DBObject keys , String name , boolean unique ){
        DBObject options = defaultOptions( keys );
        if (name != null && name.length()>0)
            options.put( "name" , name );
        if ( unique )
            options.put( "unique" , Boolean.TRUE );
        ensureIndex( keys , options );
    }

    /**
     * Creates an index on a set of fields, if one does not already exist.
     *
     * @param keys      an object with a key set of the fields desired for the index
     * @param optionsIN options for the index (name, unique, etc)
     * @throws MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link DBCollection#createIndex(DBObject, DBObject)} instead
     */
    @Deprecated
    public void ensureIndex( final DBObject keys , final DBObject optionsIN ){

        if ( checkReadOnly( false ) ) return;

        final DBObject options = defaultOptions( keys );
        for ( String k : optionsIN.keySet() )
            options.put( k , optionsIN.get( k ) );

        final String name = options.get( "name" ).toString();

        if ( _createdIndexes.contains( name ) )
            return;

        createIndex( keys , options );
        _createdIndexes.add( name );
    }

    /**
     * Clears all indices that have not yet been applied to this collection.
     * @deprecated This will be removed in 3.0
     */
    @Deprecated
    public void resetIndexCache(){
        _createdIndexes.clear();
    }

    DBObject defaultOptions( DBObject keys ){
        DBObject o = new BasicDBObject();
        o.put( "name" , genIndexName( keys ) );
        o.put( "ns" , _fullName );
        return o;
    }

    /**
     * Convenience method to generate an index name from the set of fields it is over.
     * @param keys the names of the fields used in this index
     * @return a string representation of this index's fields
     *
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public static String genIndexName( DBObject keys ){
        StringBuilder name = new StringBuilder();
        for ( String s : keys.keySet() ){
            if ( name.length() > 0 )
                name.append( '_' );
            name.append( s ).append( '_' );
            Object val = keys.get( s );
            if ( val instanceof Number || val instanceof String )
                name.append( val.toString().replace( ' ', '_' ) );
        }
        return name.toString();
    }

    // --- END INDEX CODE ---

    /**
     * Set hint fields for this collection (to optimize queries).
     * @param lst a list of {@code DBObject}s to be used as hints
     */
    public void setHintFields( List<DBObject> lst ){
        _hintFields = lst;
    }

    /**
     * Get hint fields for this collection (used to optimize queries).
     * @return a list of {@code DBObject} to be used as hints.
     */
    protected List<DBObject> getHintFields() {
        return _hintFields;
    }

    /**
     * Queries for an object in this collection.
     *
     * @param ref A document outlining the search query
     * @return an iterator over the results
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBCursor find( DBObject ref ){
        return new DBCursor( this, ref, null, getReadPreference());
    }

    /**
     * Queries for an object in this collection.
     * <p>
     * An empty DBObject will match every document in the collection.
     * Regardless of fields specified, the _id fields are always returned.
     * </p>
     * <p>
     * An example that returns the "x" and "_id" fields for every document
     * in the collection that has an "x" field:
     * </p>
     * <pre>
     * {@code
     * BasicDBObject keys = new BasicDBObject();
     * keys.put("x", 1);
     *
     * DBCursor cursor = collection.find(new BasicDBObject(), keys);}
     * </pre>
     *
     * @param ref object for which to search
     * @param keys fields to return
     * @return a cursor to iterate over results
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBCursor find( DBObject ref , DBObject keys ){
        return new DBCursor( this, ref, keys, getReadPreference());
    }


    /**
     * Queries for all objects in this collection.
     *
     * @return a cursor which will iterate over every object
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBCursor find(){
        return new DBCursor( this, null, null, getReadPreference());
    }

    /**
     * Returns a single object from this collection.
     *
     * @return the object found, or {@code null} if the collection is empty
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne(){
        return findOne( new BasicDBObject() );
    }

    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @return the object found, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( DBObject o ){
        return findOne( o, null, null, getReadPreference());
    }

    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @param fields fields to return
     * @return the object found, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( DBObject o, DBObject fields ) {
        return findOne( o, fields, null, getReadPreference());
    }
    
    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @param fields fields to return
     * @param orderBy fields to order by
     * @return the object found, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( DBObject o, DBObject fields, DBObject orderBy){
    	return findOne(o, fields, orderBy, getReadPreference());
    }

    /**
     * Get a single document from collection.
     *
     * @param o        the selection criteria using query operators.
     * @param fields   specifies which fields MongoDB will return from the documents in the result set.
     * @param readPref {@link ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( DBObject o, DBObject fields, ReadPreference readPref ){
       return findOne(o, fields, null, readPref);
    }

    /**
     * Get a single document from collection.
     *
     * @param o        the selection criteria using query operators.
     * @param fields   specifies which projection MongoDB will return from the documents in the result set.
     * @param orderBy  A document whose fields specify the attributes on which to sort the result set.
     * @param readPref {@code ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne(DBObject o, DBObject fields, DBObject orderBy, ReadPreference readPref) {
        return findOne(o, fields, orderBy, readPref, 0, MILLISECONDS);
    }

    /**
     * Get a single document from collection.
     *
     * @param o           the selection criteria using query operators.
     * @param fields      specifies which projection MongoDB will return from the documents in the result set.
     * @param orderBy     A document whose fields specify the attributes on which to sort the result set.
     * @param readPref    {@code ReadPreference} to be used for this operation
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Query
     * @since 2.12.0
     */
    DBObject findOne(DBObject o, DBObject fields, DBObject orderBy, ReadPreference readPref,
                     long maxTime, TimeUnit maxTimeUnit) {

        QueryOpBuilder queryOpBuilder = new QueryOpBuilder().addQuery(o).addOrderBy(orderBy)
                                                            .addMaxTimeMS(MILLISECONDS.convert(maxTime, maxTimeUnit));

        if (getDB().getMongo().isMongosConnection()) {
            queryOpBuilder.addReadPreference(readPref);
        }

        Iterator<DBObject> i = find(queryOpBuilder.get(), fields, 0, -1, 0, getOptions(), readPref, getDecoder());
        
        DBObject obj = (i.hasNext() ? i.next() : null);
        if ( obj != null && ( fields != null && fields.keySet().size() > 0 ) ){
            obj.markAsPartialObject();
        }
        return obj;
    }

    // Only create a new decoder if there is a decoder factory explicitly set on the collection.  Otherwise return null
    // so that DBPort will use a cached decoder from the default factory.
    DBDecoder getDecoder() {
        return getDBDecoderFactory() != null ? getDBDecoderFactory().create() : null;
    }

    // Only create a new encoder if there is an encoder factory explicitly set on the collection.  Otherwise return null
    // to allow DB to create its own or use a cached one.
    private DBEncoder getDBEncoder() {
        return getDBEncoderFactory() != null ? getDBEncoderFactory().create() : null;
    }


    /**
     * calls {@link DBCollection#apply(com.mongodb.DBObject, boolean)} with ensureID=true
     * @param o {@code DBObject} to which to add fields
     * @return the modified parameter object
     */
    public Object apply( DBObject o ){
        return apply( o , true );
    }

    /**
     * calls {@link DBCollection#doapply(com.mongodb.DBObject)}, optionally adding an automatic _id field
     * @param jo object to add fields to
     * @param ensureID whether to add an {@code _id} field
     * @return the modified object {@code o}
     */
    public Object apply( DBObject jo , boolean ensureID ){
        Object id = jo.get("_id");
        if (ensureID && id == null) {
            id = ObjectId.get();
            jo.put("_id", id);
        }

        doapply(jo);

        return id;
    }

    /**
     * Update an existing document or insert a document depending on the parameter. If the document does not contain an '_id' field, then
     * the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectid value. If
     * the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field: <ul> <li>If a
     * document does not exist with the specified '_id' value, the method performs an insert with the specified fields in the document.</li>
     * <li>If a document exists with the specified '_id' value, the method performs an update, replacing all field in the existing record
     * with the fields from the document.</li> </ul>. Calls {@link DBCollection#save(com.mongodb.DBObject, com.mongodb.WriteConcern)} with
     * default WriteConcern
     *
     * @param jo {@link DBObject} to save to the collection.
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual tutorial/modify-documents/#modify-a-document-with-save-method Save
     */
    public WriteResult save( DBObject jo ){
    	return save(jo, getWriteConcern());
    }

    /**
     * Update an existing document or insert a document depending on the parameter. If the document does not contain an '_id' field, then
     * the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectid value. If
     * the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field: <ul> <li>If a
     * document does not exist with the specified '_id' value, the method performs an insert with the specified fields in the document.</li>
     * <li>If a document exists with the specified '_id' value, the method performs an update, replacing all field in the existing record
     * with the fields from the document.</li> </ul>
     *
     * @param jo      {@link DBObject} to save to the collection.
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual tutorial/modify-documents/#modify-a-document-with-save-method Save
     */
    public WriteResult save( DBObject jo, WriteConcern concern ){
        if ( checkReadOnly( true ) )
            return null;

        _checkObject( jo , false , false );

        Object id = jo.get( "_id" );

        if (id == null || (id instanceof ObjectId && ((ObjectId) id).isNew())) {
            if (id != null) {
                ((ObjectId) id).notNew();
            }
            if (concern == null) {
                return insert(jo);
            } else {
                return insert(jo, concern);
            }
        }

        DBObject q = new BasicDBObject();
        q.put("_id", id);
        if (concern == null) {
            return update(q, jo, true, false);
        } else {
            return update(q, jo, true, false, concern);
        }

    }

    // ---- DB COMMANDS ----
    /**
     * Drops all indices from this collection
     * @throws MongoException
     */
    public void dropIndexes(){
        dropIndexes( "*" );
    }


    /**
     * Drops an index from this collection
     * @param name the index name
     * @throws MongoException
     */
    public void dropIndexes( String name ){
        DBObject cmd = BasicDBObjectBuilder.start()
            .add( "deleteIndexes" , getName() )
            .add( "index" , name )
            .get();

        resetIndexCache();
        CommandResult res = _db.command( cmd );
        if (res.ok() || res.getErrorMessage().equals( "ns not found" ))
            return;
        res.throwOnError();
    }

    /**
     * Drops (deletes) this collection. Use with care.
     * @throws MongoException
     */
    public void drop(){
        resetIndexCache();
        CommandResult res =_db.command( BasicDBObjectBuilder.start().add( "drop" , getName() ).get() );
        if (res.ok() || res.getErrorMessage().equals( "ns not found" ))
            return;
        res.throwOnError();
    }

    /**
     * Get the number of documents in the collection.
     *
     * @return the number of documents
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count(){
        return getCount(new BasicDBObject(), null);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count(DBObject query){
        return getCount(query, null);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query     specifies the selection criteria
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count(DBObject query, ReadPreference readPrefs ){
        return getCount(query, null, readPrefs);
    }


    /**
     * Get the count of documents in a collection.  Calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject)} with an
     * empty query and null fields.
     *
     * @return the number of documents in the collection
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(){
        return getCount(new BasicDBObject(), null);
    }

    /**
     * Get the count of documents in a collection. Calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject,
     * com.mongodb.ReadPreference)} with empty query and null fields.
     *
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(ReadPreference readPrefs){
        return getCount(new BasicDBObject(), null, readPrefs);
    }

    /**
     * Get the count of documents in collection that would match a criteria. Calls {@link DBCollection#getCount(com.mongodb.DBObject,
     * com.mongodb.DBObject)} with null fields.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(DBObject query){
        return getCount(query, null);
    }


    /**
     * Get the count of documents in collection that would match a criteria. Calls {@link DBCollection#getCount(com.mongodb.DBObject,
     * com.mongodb.DBObject, long, long)} with limit=0 and skip=0
     *
     * @param query  specifies the selection criteria
     * @param fields this is ignored
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(DBObject query, DBObject fields){
        return getCount( query , fields , 0 , 0 );
    }

    /**
     * Get the count of documents in collection that would match a criteria.  Calls {@link DBCollection#getCount(com.mongodb.DBObject,
     * com.mongodb.DBObject, long, long, com.mongodb.ReadPreference)} with limit=0 and skip=0
     *
     * @param query     specifies the selection criteria
     * @param fields    this is ignored
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(DBObject query, DBObject fields, ReadPreference readPrefs){
        return getCount( query , fields , 0 , 0, readPrefs );
    }

    /**
     * Get the count of documents in collection that would match a criteria.  Calls {@link DBCollection#getCount(com.mongodb.DBObject,
     * com.mongodb.DBObject, long, long, com.mongodb.ReadPreference)} with the DBCollection's ReadPreference
     *
     * @param query          specifies the selection criteria
     * @param fields     this is ignored
     * @param limit          limit the count to this value
     * @param skip           number of documents to skip
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(DBObject query, DBObject fields, long limit, long skip){
    	return getCount(query, fields, limit, skip, getReadPreference());
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query     specifies the selection criteria
     * @param fields    this is ignored
     * @param limit     limit the count to this value
     * @param skip      number of documents to skip
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(DBObject query, DBObject fields, long limit, long skip, ReadPreference readPrefs ){
        return getCount(query, fields, limit, skip, readPrefs, 0, MILLISECONDS);
    }

    long getCount(final DBObject query, final DBObject fields, final long limit, final long skip,
                  final ReadPreference readPrefs, final long maxTime, final TimeUnit maxTimeUnit) {
        return getCount(query, fields, limit, skip, readPrefs, maxTime, maxTimeUnit, null);
    }

    long getCount(final DBObject query, final DBObject fields, final long limit, final long skip,
        final ReadPreference readPrefs, final long maxTime, final TimeUnit maxTimeUnit, final Object hint) {
        BasicDBObject cmd = new BasicDBObject();
        cmd.put("count", getName());
        cmd.put("query", query);
        if (fields != null) {
            cmd.put("fields", fields);
        }
        if ( limit > 0 )
            cmd.put( "limit" , limit );
        if ( skip > 0 )
            cmd.put( "skip" , skip );
        if (maxTime > 0) {
            cmd.put("maxTimeMS", MILLISECONDS.convert(maxTime, maxTimeUnit));
        }
        if (hint != null) {
            cmd.put("hint", hint);
        }

        CommandResult res = _db.command(cmd, getOptions(), readPrefs);
        if ( ! res.ok() ){
            String errmsg = res.getErrorMessage();

            if ( errmsg.equals("ns does not exist") ||
                 errmsg.equals("ns missing" ) ){
                // for now, return 0 - lets pretend it does exist
                return 0;
            }

            res.throwOnError();
        }

        return res.getLong("n");
    }
    
    CommandResult command(DBObject cmd, int options, ReadPreference readPrefs){
    	return _db.command(cmd,getOptions(),readPrefs);
    }

    /**
     * Calls {@link DBCollection#rename(java.lang.String, boolean)} with dropTarget=false
     * @param newName new collection name (not a full namespace)
     * @return the new collection
     * @throws MongoException
     */
    public DBCollection rename( String newName ){
        return rename(newName, false);
    }

    /**
     * Renames of this collection to newName
     * @param newName new collection name (not a full namespace)
     * @param dropTarget if a collection with the new name exists, whether or not to drop it
     * @return the new collection
     * @throws MongoException
     */
    public DBCollection rename( String newName, boolean dropTarget ){
        CommandResult ret =
            _db.getSisterDB( "admin" )
            .command( BasicDBObjectBuilder.start()
                      .add( "renameCollection" , _fullName )
                      .add( "to" , _db._name + "." + newName )
                      .add( "dropTarget" , dropTarget )
                      .get() );
        ret.throwOnError();
        resetIndexCache();
        return _db.getCollection( newName );
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL. Calls {@link DBCollection#group(com.mongodb.DBObject,
     * com.mongodb.DBObject, com.mongodb.DBObject, java.lang.String, java.lang.String)} with finalize=null
     *
     * @param key     specifies one or more document fields to group
     * @param cond    specifies the selection criteria to determine which documents in the collection to process
     * @param initial initializes the aggregation result document
     * @param reduce  specifies an $reduce Javascript function, that operates on the documents during the grouping operation
     * @return a document with the grouped records as well as the command meta-data
     * @throws MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group( DBObject key , DBObject cond , DBObject initial , String reduce ){
        return group( key , cond , initial , reduce , null );
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
     * @throws MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group( DBObject key , DBObject cond , DBObject initial , String reduce , String finalize ){
        GroupCommand cmd = new GroupCommand(this, key, cond, initial, reduce, finalize);
        return group( cmd );
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param key       specifies one or more document fields to group
     * @param cond      specifies the selection criteria to determine which documents in the collection to process
     * @param initial   initializes the aggregation result document
     * @param reduce    specifies an $reduce Javascript function, that operates on the documents during the grouping operation
     * @param finalize  specifies a Javascript function that runs each item in the result set before final value will be returned
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return a document with the grouped records as well as the command meta-data
     * @throws MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group( DBObject key , DBObject cond , DBObject initial , String reduce , String finalize, ReadPreference readPrefs ){
        GroupCommand cmd = new GroupCommand(this, key, cond, initial, reduce, finalize);
        return group( cmd, readPrefs );
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param cmd the group command containing the details of how to perform the operation.
     * @return a document with the grouped records as well as the command meta-data
     * @throws MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group( GroupCommand cmd ) {
        return group(cmd, getReadPreference());
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param cmd       the group command containing the details of how to perform the operation.
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return a document with the grouped records as well as the command meta-data
     * @throws MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    public DBObject group( GroupCommand cmd, ReadPreference readPrefs ) {
        CommandResult res =  _db.command( cmd.toDBObject(), getOptions(), readPrefs );
        res.throwOnError();
        return (DBObject)res.get( "retval" );
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param args object representing the arguments to the group function
     * @return a document with the grouped records as well as the command meta-data
     * @throws MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     * @deprecated use {@link DBCollection#group(com.mongodb.GroupCommand)} instead.  This method will be removed in 3.0
     */
    @Deprecated
    public DBObject group( DBObject args ){
        args.put( "ns" , getName() );
        CommandResult res =  _db.command( new BasicDBObject( "group" , args ), getOptions(), getReadPreference() );
        res.throwOnError();
        return (DBObject)res.get( "retval" );
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param key Specifies the field for which to return the distinct values
     * @return A {@code List} of the distinct values
     * @throws MongoException
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct( String key ){
        return distinct( key , new BasicDBObject() );
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param key       Specifies the field for which to return the distinct values
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return A {@code List} of the distinct values
     * @throws MongoException
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct( String key, ReadPreference readPrefs ){
        return distinct( key , new BasicDBObject(), readPrefs );
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param key   Specifies the field for which to return the distinct values
     * @param query specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @return A {@code List} of the distinct values
     * @throws MongoException
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct( String key , DBObject query ){
         return distinct(key, query, getReadPreference());
     }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param key       Specifies the field for which to return the distinct values
     * @param query     specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return A {@code List} of the distinct values
     * @throws MongoException
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct( String key , DBObject query, ReadPreference readPrefs ){
        DBObject c = BasicDBObjectBuilder.start()
            .add( "distinct" , getName() )
            .add( "key" , key )
            .add( "query" , query )
            .get();

        CommandResult res = _db.command( c, getOptions(), readPrefs );
        res.throwOnError();
        return (List)(res.get( "values" ));
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection.  Runs the command in REPLACE output mode (saves to named
     * collection).
     *
     * @param map            a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce         a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget   specifies the location of the result of the map-reduce operation (optional) - leave null if want to use temp
     *                       collection
     * @param query          specifies the selection criteria using query operators for determining the documents input to the map
     *                       function.
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce( String map , String reduce , String outputTarget , DBObject query ){
        return mapReduce( new MapReduceCommand( this , map , reduce , outputTarget , MapReduceCommand.OutputType.REPLACE, query ) );
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     * Specify an outputType to control job execution<ul>
     * <li>INLINE - Return results inline</li>
     * <li>REPLACE - Replace the output collection with the job output</li>
     * <li>MERGE - Merge the job output with the existing contents of outputTarget</li>
     * <li>REDUCE - Reduce the job output with the existing contents of outputTarget</li>
     * </ul>
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation (optional) - leave null if want to use temp
     *                     collection
     * @param outputType   specifies the type of job output
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(String map, String reduce, String outputTarget, MapReduceCommand.OutputType outputType,
                                     DBObject query) {
        return mapReduce( new MapReduceCommand( this , map , reduce , outputTarget , outputType , query ) );
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     * Specify an outputType to control job execution<ul>
     * <li>INLINE - Return results inline</li>
     * <li>REPLACE - Replace the output collection with the job output</li>
     * <li>MERGE - Merge the job output with the existing contents of outputTarget</li>
     * <li>REDUCE - Reduce the job output with the existing contents of outputTarget</li>
     * </ul>
     *
     * @param map            a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce         a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget   specifies the location of the result of the map-reduce operation (optional) - leave null if want to use temp
     *                       collection
     * @param outputType     specifies the type of job output
     * @param query          specifies the selection criteria using query operators for determining the documents input to the map
     *                       function.
     * @param readPrefs the read preference specifying where to run the query.  Only applied for Inline output type
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(String map, String reduce, String outputTarget, MapReduceCommand.OutputType outputType, DBObject query,
                                     ReadPreference readPrefs) {
        MapReduceCommand command = new MapReduceCommand( this , map , reduce , outputTarget , outputType , query );
        command.setReadPreference(readPrefs);
        return mapReduce( command );
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     *
     * @param command object representing the parameters to the operation
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce( MapReduceCommand command ){
        DBObject cmd = command.toDBObject();
        // if type in inline, then query options like slaveOk is fine
        CommandResult res = _db.command( cmd, getOptions(), command.getReadPreference() != null ? command.getReadPreference() : getReadPreference() );
        res.throwOnError();
        return new MapReduceOutput( this , cmd, res );
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection
     *
     * @param command document representing the parameters to this operation.
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     * @deprecated Use {@link com.mongodb.DBCollection#mapReduce(MapReduceCommand)} instead
     */
    @Deprecated
    public MapReduceOutput mapReduce( DBObject command ){
        if ( command.get( "mapreduce" ) == null && command.get( "mapReduce" ) == null )
            throw new IllegalArgumentException( "need mapreduce arg" );
        CommandResult res = _db.command( command, getOptions(), getReadPreference() );
        res.throwOnError();
        return new MapReduceOutput( this , command, res );
    }
    
    /**
     * Method implements aggregation framework.
     *
     * @param firstOp       requisite first operation to be performed in the aggregation pipeline
     * @param additionalOps additional operations to be performed in the aggregation pipeline
     * @return the aggregation operation's result set
     * @deprecated Use {@link com.mongodb.DBCollection#aggregate(java.util.List)} instead
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     *
     * @mongodb.server.release 2.2
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
     *
     * @mongodb.server.release 2.2
     */
    public AggregationOutput aggregate(final List<DBObject> pipeline) {
        return aggregate(pipeline, getReadPreference());
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline       operations to be performed in the aggregation pipeline
     * @param readPreference the read preference specifying where to run the query
     * @return the aggregation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     *
     * @mongodb.server.release 2.2
     */
    public AggregationOutput aggregate(final List<DBObject> pipeline, ReadPreference readPreference) {
        AggregationOptions options = AggregationOptions.builder()
                .outputMode(AggregationOptions.OutputMode.INLINE)
                .build();

        DBObject command = prepareCommand(pipeline, options);

        CommandResult res = _db.command(command, getOptions(), readPreference);
        res.throwOnError();
        
        return new AggregationOutput(command, res);
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline operations to be performed in the aggregation pipeline
     * @param options  options to apply to the aggregation
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     *
     * @mongodb.server.release 2.2
     */
    public Cursor aggregate(final List<DBObject> pipeline, AggregationOptions options) {
        return aggregate(pipeline, options, getReadPreference());
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline operations to be performed in the aggregation pipeline
     * @param options options to apply to the aggregation
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     *
     * @mongodb.server.release 2.2
     */
    public abstract Cursor aggregate(final List<DBObject> pipeline, final AggregationOptions options,
                                          final ReadPreference readPreference);

    /**
     * Return the explain plan for the aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline to explain
     * @param options  the options to apply to the aggregation
     * @return the command result.  The explain output may change from release to release, so best to simply log this.
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.driver.manual reference/operator/meta/explain/ Explain query
     *
     * @mongodb.server.release 2.6
     */
    public CommandResult explainAggregate(List<DBObject> pipeline, AggregationOptions options) {
        DBObject command = prepareCommand(pipeline, options);
        command.put("explain", true);
        final CommandResult res = _db.command(command, getOptions(), getReadPreference());
        res.throwOnError();
        
        return res;
    }

    /**
     * Return a list of cursors over the collection that can be used to scan it in parallel.
     * <p>
     *     Note: As of MongoDB 2.6, this method will work against a mongod, but not a mongos.
     * </p>
     *
     * @param options the parallel scan options
     * @return a list of cursors, whose size may be less than the number requested
     * @since 2.12
     *
     * @mongodb.server.release 2.6
     */
    public abstract List<Cursor> parallelScan(final ParallelScanOptions options);

    /**
     * Creates a builder for an ordered bulk write operation, consisting of an ordered collection of write requests,
     * which can be any combination of inserts, updates, replaces, or removes. Write requests included in the bulk operations will be
     * executed in order, and will halt on the first failure.
     * <p>
     * Note: While this bulk write operation will execute on MongoDB 2.4 servers and below, the writes will be performed one at a time,
     * as that is the only way to preserve the semantics of the value returned from execution or the exception thrown.
     * <p>
     * Note: While a bulk write operation with a mix of inserts, updates, replaces, and removes is supported,
     * the implementation will batch up consecutive requests of the same type and send them to the server one at a time.  For example,
     * if a bulk write operation consists of 10 inserts followed by 5 updates, followed by 10 more inserts,
     * it will result in three round trips to the server.
     *
     * @return the builder
     *
     * @since 2.12
     */
    public BulkWriteOperation initializeOrderedBulkOperation() {
        return new BulkWriteOperation(true, this);
    }

    /**
     * Creates a builder for an unordered bulk operation, consisting of an unordered collection of write requests,
     * which can be any combination of inserts, updates, replaces, or removes. Write requests included in the bulk operation will be
     * executed in an undefined  order, and all requests will be executed even if some fail.
     * <p>
     * Note: While this bulk write operation will execute on MongoDB 2.4 servers and below, the writes will be performed one at a time,
     * as that is the only way to preserve the semantics of the value returned from execution or the exception thrown.
     *
     * @return the builder
     *
     * @since 2.12
     */
    public BulkWriteOperation initializeUnorderedBulkOperation() {
        return new BulkWriteOperation(false, this);
    }

    BulkWriteResult executeBulkWriteOperation(final boolean ordered, final List<WriteRequest> requests) {
        return executeBulkWriteOperation(ordered, requests, getWriteConcern());
    }

    BulkWriteResult executeBulkWriteOperation(final boolean ordered, final List<WriteRequest> requests, final WriteConcern writeConcern) {
        return executeBulkWriteOperation(ordered, requests, writeConcern, getDBEncoder());
    }

    abstract BulkWriteResult executeBulkWriteOperation(final boolean ordered, final List<WriteRequest> requests,
                                                       final WriteConcern writeConcern, final DBEncoder encoder);

    @SuppressWarnings("unchecked")
    DBObject prepareCommand(final List<DBObject> pipeline, final AggregationOptions options) {
        if (pipeline.isEmpty()) {
            throw new MongoException("Aggregation pipelines can not be empty");
        }

        DBObject command = new BasicDBObject("aggregate", getName());
        command.put("pipeline", pipeline);

        if (options.getOutputMode() == AggregationOptions.OutputMode.CURSOR) {
            BasicDBObject cursor = new BasicDBObject();
            if (options.getBatchSize() != null) {
                cursor.put("batchSize", options.getBatchSize());
            }
            command.put("cursor", cursor);
        }
        if (options.getMaxTime(MILLISECONDS) > 0) {
            command.put("maxTimeMS", options.getMaxTime(MILLISECONDS));
        }

        if (options.getAllowDiskUse() != null) {
            command.put("allowDiskUse", options.getAllowDiskUse());
        }

        return command;
    }

    /**
     * Return a list of the indexes for this collection.  Each object in the list is the "info document" from MongoDB
     *
     * @return list of index documents
     * @throws MongoException
     */
    public List<DBObject> getIndexInfo() {
        BasicDBObject cmd = new BasicDBObject();
        cmd.put("ns", getFullName());

        DBCursor cur = _db.getCollection("system.indexes").find(cmd);

        List<DBObject> list = new ArrayList<DBObject>();

        while(cur.hasNext()) {
            list.add(cur.next());
        }

        return list;
    }

    /**
     * Drops an index from this collection
     * @param keys keys of the index
     * @throws MongoException
     */
    public void dropIndex( DBObject keys ){
        dropIndexes( genIndexName( keys ) );
    }

    /**
     * Drops an index from this collection
     * @param name name of index to drop
     * @throws MongoException
     */
    public void dropIndex( String name ){
        dropIndexes( name );
    }

    /**
     * The collStats command returns a variety of storage statistics for a given collection
     *
     * @return a CommandResult containing the statistics about this collection
     * @mongodb.driver.manual /reference/command/collStats/ collStats command
     */
    public CommandResult getStats() {
        return getDB().command(new BasicDBObject("collstats", getName()), getOptions(), getReadPreference());
    }

    /**
     * Checks whether this collection is capped
     *
     * @return true if this is a capped collection
     * @throws MongoException
     * @mongodb.driver.manual /core/capped-collections/#check-if-a-collection-is-capped Capped Collections
     */
    public boolean isCapped() {
        CommandResult stats = getStats();
        Object capped = stats.get("capped");
        return(capped != null && ( capped.equals(1) || capped.equals(true) ) );
    }

    // ------

    /**
     * Initializes a new collection. No operation is actually performed on the database.
     * @param base database in which to create the collection
     * @param name the name of the collection
     */
    protected DBCollection( DB base , String name ){
        _db = base;
        _name = name;
        _fullName = _db.getName() + "." + name;
        _options = new Bytes.OptionHolder( _db._options );
    }

    /**
     * @deprecated This method should not be a part of API.
     *             If you override one of the {@code DBCollection} methods please rely on superclass
     *             implementation in checking argument correctness and validity.
     */
    @Deprecated
    protected DBObject _checkObject(DBObject o, boolean canBeNull, boolean query) {
        if (o == null) {
            if (canBeNull)
                return null;
            throw new IllegalArgumentException("can't be null");
        }

        if (o.isPartialObject() && !query)
            throw new IllegalArgumentException("can't save partial objects");

        if (!query) {
            _checkKeys(o);
        }
        return o;
    }

    /**
     * Checks key strings for invalid characters.
     */
    private void _checkKeys( DBObject o ) {
        if ( o instanceof LazyDBObject || o instanceof LazyDBList )
            return;

        for ( String s : o.keySet() ){
            validateKey( s );
            _checkValue( o.get( s ) );
        }
    }

    /**
     * Checks key strings for invalid characters.
     */
    private void _checkKeys( Map<String, Object> o ) {
        for ( Map.Entry<String, Object> cur : o.entrySet() ){
            validateKey( cur.getKey() );
            _checkValue( cur.getValue() );
        }
    }

    private void _checkValues( final List list ) {
        for ( Object cur : list ) {
            _checkValue( cur );
        }
    }

    private void _checkValue(final Object value) {
        if ( value instanceof DBObject ) {
            _checkKeys( (DBObject)value );
        } else if ( value instanceof Map ) {
            _checkKeys( (Map<String, Object>)value );
        } else if ( value instanceof List ) {
            _checkValues((List) value);
        }
    }

    /**
     * Check for invalid key names
     * @param s the string field/key to check
     * @exception IllegalArgumentException if the key is not valid.
     */
    private void validateKey(String s ) {
        if ( s.contains( "\0" ) )
            throw new IllegalArgumentException( "Document field names can't have a NULL character. (Bad Key: '" + s + "')" );
        if ( s.contains( "." ) )
            throw new IllegalArgumentException( "Document field names can't have a . in them. (Bad Key: '" + s + "')" );
        if ( s.startsWith( "$" ) )
            throw new IllegalArgumentException( "Document field names can't start with '$' (Bad Key: '" + s + "')" );
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
     * @param n the name of the collection to find
     * @return the matching collection
     */
    public DBCollection getCollection( String n ){
        return _db.getCollection( _name + "." + n );
    }

    /**
     * Returns the name of this collection.
     * @return  the name of this collection
     */
    public String getName(){
        return _name;
    }

    /**
     * Returns the full name of this collection, with the database name as a prefix.
     * @return  the name of this collection
     */
    public String getFullName(){
        return _fullName;
    }

    /**
     * Returns the database this collection is a member of.
     * @return this collection's database
     */
    public DB getDB(){
        return _db;
    }

    /**
     * Returns if this collection's database is read-only
     *
     * @param strict if an exception should be thrown if the database is read-only
     * @return if this collection's database is read-only
     * @throws RuntimeException if the database is read-only and {@code strict} is set
     * @deprecated See {@link DB#setReadOnly(Boolean)}
     */
    @Deprecated
    protected boolean checkReadOnly( boolean strict ){
        if ( ! _db._readOnly )
            return false;

        if ( ! strict )
            return true;

        throw new IllegalStateException( "db is read only" );
    }

    @Override
    public int hashCode(){
        return _fullName.hashCode();
    }

    @Override
    public boolean equals( Object o ){
        return o == this;
    }

    @Override
    public String toString(){
        return _name;
    }

    /**
     * Sets a default class for objects in this collection; null resets the class to nothing.
     * @param c the class
     * @throws IllegalArgumentException if {@code c} is not a DBObject
     */
    public void setObjectClass( Class c ){
        if ( c == null ){
            // reset
            _wrapper = null;
            _objectClass = null;
            return;
        }

        if ( ! DBObject.class.isAssignableFrom( c ) )
            throw new IllegalArgumentException( c.getName() + " is not a DBObject" );
        _objectClass = c;
        if ( ReflectionDBObject.class.isAssignableFrom( c ) )
            _wrapper = ReflectionDBObject.getWrapper( c );
        else
            _wrapper = null;
    }

    /**
     * Gets the default class for objects in the collection
     *
     * @return the class
     */
    public Class getObjectClass(){
        return _objectClass;
    }

    /**
     * Sets the internal class for the given path in the document hierarchy
     *
     * @param path the path to map the given Class to
     * @param c    the Class to map the given path to
     */
    public void setInternalClass( String path , Class c ){
        _internalClass.put( path , c );
    }

    /**
     * Gets the internal class for the given path in the document hierarchy
     *
     * @param path the path to map the given Class to
     * @return the class for a given path in the hierarchy
     */
    protected Class getInternalClass( String path ){
        Class c = _internalClass.get( path );
        if ( c != null )
            return c;

        if ( _wrapper == null )
            return null;
        return _wrapper.getInternalClass( path );
    }

    /**
     * Set the write concern for this collection. Will be used for
     * writes to this collection. Overrides any setting of write
     * concern at the DB level. See the documentation for
     * {@link WriteConcern} for more information.
     *
     * @param concern write concern to use
     */
    public void setWriteConcern( WriteConcern concern ){
        _concern = concern;
    }

    /**
     * Get the {@link WriteConcern} for this collection.
     *
     * @return the default write concern for this collection
     */
    public WriteConcern getWriteConcern(){
        if ( _concern != null )
            return _concern;
        return _db.getWriteConcern();
    }

    /**
     * Sets the read preference for this collection. Will be used as default
     * for reads from this collection; overrides DB & Connection level settings.
     * See the * documentation for {@link ReadPreference} for more information.
     *
     * @param preference Read Preference to use
     */
    public void setReadPreference( ReadPreference preference ){
        _readPref = preference;
    }

    /**
     * Gets the {@link ReadPreference}.
     *
     * @return the default read preference for this collection
     */
    public ReadPreference getReadPreference(){
        if ( _readPref != null )
            return _readPref;
        return _db.getReadPreference();
    }
    /**
     * Makes this query ok to run on a slave node
     *
     * @deprecated Replaced with {@link ReadPreference#secondaryPreferred()}
     */
    @Deprecated
    public void slaveOk(){
        addOption(Bytes.QUERYOPTION_SLAVEOK);
    }

    /**
     * Adds the given flag to the query options.
     *
     * @param option value to be added
     */
    public void addOption( int option ){
        _options.add(option);
    }

    /**
     * Sets the query options, overwriting previous value.
     *
     * @param options bit vector of query options
     */
    public void setOptions( int options ){
        _options.set(options);
    }

    /**
     * Resets the default query options
     */
    public void resetOptions(){
        _options.reset();
    }

    /**
     * Gets the default query options
     *
     * @return bit vector of query options
     */
    public int getOptions(){
        return _options.get();
    }

    /**
     * Set a customer decoder factory for this collection.  Set to null to use the default from MongoOptions.
     *
     * @param fact the factory to set.
     */
    public synchronized void setDBDecoderFactory(DBDecoderFactory fact) {
        _decoderFactory = fact;
    }

    /**
     * Get the decoder factory for this collection.  A null return value means that the default from MongoOptions is being used.
     *
     * @return the factory
     */
    public synchronized DBDecoderFactory getDBDecoderFactory() {
        return _decoderFactory;
    }

    /**
     * Set a customer encoder factory for this collection.  Set to null to use the default from MongoOptions.
     *
     * @param fact the factory to set.
     */
    public synchronized void setDBEncoderFactory(DBEncoderFactory fact) {
        _encoderFactory = fact;
    }

    /**
     * Get the encoder factory for this collection.  A null return value means that the default from MongoOptions is being used.
     *
     * @return the factory
     */
    public synchronized DBEncoderFactory getDBEncoderFactory() {
        return _encoderFactory;
    }

    final DB _db;

    /**
     * @deprecated Please use {@link #getName()} instead.
     */
    @Deprecated
    final protected String _name;

    /**
     * @deprecated Please use {@link #getFullName()} instead.
     */
    @Deprecated
    final protected String _fullName;

    /**
     * @deprecated Please use {@link #setHintFields(java.util.List)} and {@link #getHintFields()} instead.
     */
    @Deprecated
    protected List<DBObject> _hintFields;
    private WriteConcern _concern = null;
    private ReadPreference _readPref = null;
    private DBDecoderFactory _decoderFactory;
    private DBEncoderFactory _encoderFactory;
    final Bytes.OptionHolder _options;

    /**
     * @deprecated Please use {@link #getObjectClass()} and {@link #setObjectClass(Class)} instead.
     */
    @Deprecated
    protected Class _objectClass = null;
    private Map<String,Class> _internalClass = Collections.synchronizedMap( new HashMap<String,Class>() );
    private ReflectionDBObject.JavaWrapper _wrapper = null;

    /**
     * @deprecated This will be removed in 3.0
     */
    @Deprecated
    final private Set<String> _createdIndexes = new HashSet<String>();

}
