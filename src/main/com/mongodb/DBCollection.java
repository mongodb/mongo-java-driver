// DBCollection.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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

/** This class provides a skeleton implementation of a database collection.
 * <p>A typical invocation sequence is thus
 * <blockquote><pre>
 *     MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
 *     DB db = mongo.getDB("mydb");
 *     DBCollection collection = db.getCollection("test");
 * </pre></blockquote>
 * @dochub collections
 */
@SuppressWarnings("unchecked")
public abstract class DBCollection {

    /**
     * Saves document(s) to the database.
     * if doc doesn't have an _id, one will be added
     * you can get the _id that was added from doc after the insert
     *
     * @param arr  array of documents to save
     * @param concern the write concern
     * @return
     * @throws MongoException
     * @dochub insert
     */
    public WriteResult insert(DBObject[] arr , WriteConcern concern ){
        return insert( arr, concern, getDBEncoder());
    }

    /**
     * Saves document(s) to the database.
     * if doc doesn't have an _id, one will be added
     * you can get the _id that was added from doc after the insert
     *
     * @param arr  array of documents to save
     * @param concern the write concern
     * @param encoder the DBEncoder to use
     * @return
     * @throws MongoException
     * @dochub insert
     */
    public WriteResult insert(DBObject[] arr , WriteConcern concern, DBEncoder encoder) {
        return insert(Arrays.asList(arr), concern, encoder);
    }

    /**
     * Inserts a document into the database.
     * if doc doesn't have an _id, one will be added
     * you can get the _id that was added from doc after the insert
     *
     * @param o
     * @param concern the write concern
     * @return
     * @throws MongoException
     * @dochub insert
     */
    public WriteResult insert(DBObject o , WriteConcern concern ){
        return insert( Arrays.asList(o) , concern );
    }

    /**
     * Saves document(s) to the database.
     * if doc doesn't have an _id, one will be added
     * you can get the _id that was added from doc after the insert
     *
     * @param arr  array of documents to save
     * @return
     * @throws MongoException
     * @dochub insert
     */
    public WriteResult insert(DBObject ... arr){
        return insert( arr , getWriteConcern() );
    }

    /**
     * Saves document(s) to the database.
     * if doc doesn't have an _id, one will be added
     * you can get the _id that was added from doc after the insert
     *
     * @param arr  array of documents to save
     * @return
     * @throws MongoException
     * @dochub insert
     */
    public WriteResult insert(WriteConcern concern, DBObject ... arr){
        return insert( arr, concern );
    }

    /**
     * Saves document(s) to the database.
     * if doc doesn't have an _id, one will be added
     * you can get the _id that was added from doc after the insert
     *
     * @param list list of documents to save
     * @return
     * @throws MongoException
     * @dochub insert
     */
    public WriteResult insert(List<DBObject> list ){
        return insert( list, getWriteConcern() );
    }

    /**
     * Saves document(s) to the database.
     * if doc doesn't have an _id, one will be added
     * you can get the _id that was added from doc after the insert
     * 
     * @param list list of documents to save
     * @param concern the write concern
     * @return
     * @throws MongoException
     * @dochub insert
     */
    public WriteResult insert(List<DBObject> list, WriteConcern concern ){
        return insert(list, concern, getDBEncoder() );
    }

    /**
     * Saves document(s) to the database.
     * if doc doesn't have an _id, one will be added
     * you can get the _id that was added from doc after the insert
     *
     * @param list list of documents to save
     * @param concern the write concern
     * @return
     * @throws MongoException
     * @dochub insert
     */
    public abstract WriteResult insert(List<DBObject> list, WriteConcern concern, DBEncoder encoder);

    /**
     * Performs an update operation.
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @param upsert if the database should create the element if it does not exist
     * @param multi if the update should be applied to all objects matching (db version 1.1.3 and above). An object will
     * not be inserted if it does not exist in the collection and upsert=true and multi=true.
     * See <a href="http://www.mongodb.org/display/DOCS/Atomic+Operations">http://www.mongodb.org/display/DOCS/Atomic+Operations</a>
     * @param concern the write concern
     * @return
     * @throws MongoException
     * @dochub update
     */
    public WriteResult update( DBObject q , DBObject o , boolean upsert , boolean multi , WriteConcern concern ){
        return update( q, o, upsert, multi, concern, getDBEncoder());
    }

    /**
     * Performs an update operation.
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @param upsert if the database should create the element if it does not exist
     * @param multi if the update should be applied to all objects matching (db version 1.1.3 and above). An object will
     * not be inserted if it does not exist in the collection and upsert=true and multi=true.
     * See <a href="http://www.mongodb.org/display/DOCS/Atomic+Operations">http://www.mongodb.org/display/DOCS/Atomic+Operations</a>
     * @param concern the write concern
     * @param encoder the DBEncoder to use
     * @return
     * @throws MongoException
     * @dochub update
     */
    public abstract WriteResult update( DBObject q , DBObject o , boolean upsert , boolean multi , WriteConcern concern, DBEncoder encoder );

    /**
     * calls {@link DBCollection#update(com.mongodb.DBObject, com.mongodb.DBObject, boolean, boolean, com.mongodb.WriteConcern)} with default WriteConcern.
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @param upsert if the database should create the element if it does not exist
     * @param multi if the update should be applied to all objects matching (db version 1.1.3 and above)
     *              See http://www.mongodb.org/display/DOCS/Atomic+Operations
     * @return
     * @throws MongoException
     * @dochub update
     */
    public WriteResult update( DBObject q , DBObject o , boolean upsert , boolean multi ){
        return update( q , o , upsert , multi , getWriteConcern() );
    }

    /**
     * calls {@link DBCollection#update(com.mongodb.DBObject, com.mongodb.DBObject, boolean, boolean)} with upsert=false and multi=false
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @return
     * @throws MongoException
     * @dochub update
     */
    public WriteResult update( DBObject q , DBObject o ){
        return update( q , o , false , false );
    }

    /**
     * calls {@link DBCollection#update(com.mongodb.DBObject, com.mongodb.DBObject, boolean, boolean)} with upsert=false and multi=true
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @return
     * @throws MongoException
     * @dochub update
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
     * Removes objects from the database collection.
     * @param o the object that documents to be removed must match
     * @param concern WriteConcern for this operation
     * @return
     * @throws MongoException
     * @dochub remove
     */
    public WriteResult remove( DBObject o , WriteConcern concern ){
        return remove(  o, concern, getDBEncoder());
    }

    /**
     * Removes objects from the database collection.
     * @param o the object that documents to be removed must match
     * @param concern WriteConcern for this operation
     * @param encoder the DBEncoder to use
     * @return
     * @throws MongoException
     * @dochub remove
     */
    public abstract WriteResult remove( DBObject o , WriteConcern concern, DBEncoder encoder );

    /**
     * calls {@link DBCollection#remove(com.mongodb.DBObject, com.mongodb.WriteConcern)} with the default WriteConcern
     * @param o the object that documents to be removed must match
     * @return
     * @throws MongoException
     * @dochub remove
     */
    public WriteResult remove( DBObject o ){
        return remove( o , getWriteConcern() );
    }


    /**
     * Finds objects
     */
    abstract Iterator<DBObject> __find( DBObject ref , DBObject fields , int numToSkip , int batchSize , int limit, int options, ReadPreference readPref, DBDecoder decoder );

    abstract Iterator<DBObject> __find( DBObject ref , DBObject fields , int numToSkip , int batchSize , int limit, int options,
                                        ReadPreference readPref, DBDecoder decoder, DBEncoder encoder );


    /**
     * Calls {@link DBCollection#find(com.mongodb.DBObject, com.mongodb.DBObject, int, int)} and applies the query options
     * @param query query used to search
     * @param fields the fields of matching objects to return
     * @param numToSkip number of objects to skip
     * @param batchSize the batch size. This option has a complex behavior, see {@link DBCursor#batchSize(int) }
     * @param options - see Bytes QUERYOPTION_*
     * @return the cursor
     * @throws MongoException
     * @dochub find
     */
    @Deprecated
    public DBCursor find( DBObject query , DBObject fields , int numToSkip , int batchSize , int options ){
    	return find(query, fields, numToSkip, batchSize).addOption(options);
    }


    /**
     * Finds objects from the database that match a query.
     * A DBCursor object is returned, that can be iterated to go through the results.
     *
     * @param query query used to search
     * @param fields the fields of matching objects to return
     * @param numToSkip number of objects to skip
     * @param batchSize the batch size. This option has a complex behavior, see {@link DBCursor#batchSize(int) }
     * @return the cursor
     * @throws MongoException
     * @dochub find
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
     * @return the object, if found, otherwise <code>null</code>
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
     * @return the object, if found, otherwise <code>null</code>
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne( Object obj, DBObject fields ){
        Iterator<DBObject> iterator = __find( new BasicDBObject("_id", obj), fields, 0, -1, 0, getOptions(), getReadPreference(), getDecoder() );
        return (iterator.hasNext() ? iterator.next() : null);
    }

    /**
     * Finds the first document in the query and updates it.
     * @param query query to match
     * @param fields fields to be returned
     * @param sort sort to apply before picking first document
     * @param remove if true, document found will be removed
     * @param update update to apply
     * @param returnNew if true, the updated document is returned, otherwise the old document is returned (or it would be lost forever)
     * @param upsert do upsert (insert if document not present)
     * @return the document
     * @throws MongoException
     */
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert){

        BasicDBObject cmd = new BasicDBObject( "findandmodify", _name);
        if (query != null && !query.keySet().isEmpty())
            cmd.append( "query", query );
        if (fields != null && !fields.keySet().isEmpty())
            cmd.append( "fields", fields );
        if (sort != null && !sort.keySet().isEmpty())
            cmd.append( "sort", sort );

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
     * calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean, com.mongodb.DBObject, boolean, boolean)}
     * with fields=null, remove=false, returnNew=false, upsert=false
     * @param query
     * @param sort
     * @param update
     * @return the old document
     * @throws MongoException
     */
    public DBObject findAndModify( DBObject query , DBObject sort , DBObject update) {
    	return findAndModify( query, null, sort, false, update, false, false);
    }

    /**
     * calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean, com.mongodb.DBObject, boolean, boolean)}
     * with fields=null, sort=null, remove=false, returnNew=false, upsert=false
     * @param query
     * @param update
     * @return the old document
     * @throws MongoException
     */
    public DBObject findAndModify( DBObject query , DBObject update ){
    	return findAndModify( query, null, null, false, update, false, false );
    }

    /**
     * calls {@link DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean, com.mongodb.DBObject, boolean, boolean)}
     * with fields=null, sort=null, remove=true, returnNew=false, upsert=false
     * @param query
     * @return the removed document
     * @throws MongoException
     */
    public DBObject findAndRemove( DBObject query ) {
    	return findAndModify( query, null, null, true, null, false, false );
    }

    // --- START INDEX CODE ---

    /**
     * calls {@link DBCollection#createIndex(com.mongodb.DBObject, com.mongodb.DBObject)} with default index options
     * @param keys an object with a key set of the fields desired for the index
     * @throws MongoException
     */
    public void createIndex( final DBObject keys ){
        createIndex( keys , defaultOptions( keys ) );
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     * @param keys
     * @param options
     * @throws MongoException
     */
    public void createIndex( DBObject keys , DBObject options ){
        createIndex( keys, options, getDBEncoder());
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     * @param keys
     * @param options
     * @param encoder the DBEncoder to use
     * @throws MongoException
     */
    public abstract void createIndex( DBObject keys , DBObject options, DBEncoder encoder );

    /**
     * Creates an ascending index on a field with default options, if one does not already exist.
     * @param name name of field to index on
     * @throws MongoException
     */
    public void ensureIndex( final String name ){
        ensureIndex( new BasicDBObject( name , 1 ) );
    }

    /**
     * calls {@link DBCollection#ensureIndex(com.mongodb.DBObject, com.mongodb.DBObject)} with default options
     * @param keys an object with a key set of the fields desired for the index
     * @throws MongoException
     */
    public void ensureIndex( final DBObject keys ){
        ensureIndex( keys , defaultOptions( keys ) );
    }

    /**
     * calls {@link DBCollection#ensureIndex(com.mongodb.DBObject, java.lang.String, boolean)} with unique=false
     * @param keys fields to use for index
     * @param name an identifier for the index
     * @throws MongoException
     * @dochub indexes
     */
    public void ensureIndex( DBObject keys , String name ){
        ensureIndex( keys , name , false );
    }

    /**
     * Ensures an index on this collection (that is, the index will be created if it does not exist).
     * @param keys fields to use for index
     * @param name an identifier for the index. If null or empty, the default name will be used.
     * @param unique if the index should be unique
     * @throws MongoException
     */
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
     * @param keys an object with a key set of the fields desired for the index
     * @param optionsIN options for the index (name, unique, etc)
     * @throws MongoException
     */
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
     */
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
     * @param lst a list of <code>DBObject</code>s to be used as hints
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
     * @param ref object for which to search
     * @return an iterator over the results
     * @dochub find
     */
    public DBCursor find( DBObject ref ){
        return new DBCursor( this, ref, null, getReadPreference());
    }

    /**
     * Queries for an object in this collection.
     *
     * <p>
     * An empty DBObject will match every document in the collection.
     * Regardless of fields specified, the _id fields are always returned.
     * </p>
     * <p>
     * An example that returns the "x" and "_id" fields for every document
     * in the collection that has an "x" field:
     * </p>
     * <blockquote><pre>
     * BasicDBObject keys = new BasicDBObject();
     * keys.put("x", 1);
     *
     * DBCursor cursor = collection.find(new BasicDBObject(), keys);
     * </pre></blockquote>
     *
     * @param ref object for which to search
     * @param keys fields to return
     * @return a cursor to iterate over results
     * @dochub find
     */
    public DBCursor find( DBObject ref , DBObject keys ){
        return new DBCursor( this, ref, keys, getReadPreference());
    }


    /**
     * Queries for all objects in this collection.
     * @return a cursor which will iterate over every object
     * @dochub find
     */
    public DBCursor find(){
        return new DBCursor( this, null, null, getReadPreference());
    }

    /**
     * Returns a single object from this collection.
     * @return the object found, or <code>null</code> if the collection is empty
     * @throws MongoException
     */
    public DBObject findOne(){
        return findOne( new BasicDBObject() );
    }

    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     */
    public DBObject findOne( DBObject o ){
        return findOne( o, null, null, getReadPreference());
    }

    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @param fields fields to return
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne( DBObject o, DBObject fields ) {
        return findOne( o, fields, null, getReadPreference());
    }
    
    /**
     * Returns a single obejct from this collection matching the query.
     * @param o the query object
     * @param fields fields to return
     * @param orderBy fields to order by
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne( DBObject o, DBObject fields, DBObject orderBy){
    	return findOne(o, fields, orderBy, getReadPreference());
    }
    
    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @param fields fields to return
     * @param readPref
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne( DBObject o, DBObject fields, ReadPreference readPref ){
       return findOne(o, fields, null, readPref);
    }
    
    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @param fields fields to return
     * @param orderBy fields to order by
     * @return the object found, or <code>null</code> if no such object exists
     * @throws MongoException
     * @dochub find
     */
    public DBObject findOne( DBObject o, DBObject fields, DBObject orderBy, ReadPreference readPref ){

        QueryOpBuilder queryOpBuilder = new QueryOpBuilder().addQuery(o).addOrderBy(orderBy);

        if (getDB().getMongo().isMongosConnection()) {
            queryOpBuilder.addReadPreference(readPref);
        }

        Iterator<DBObject> i = __find(queryOpBuilder.get(), fields , 0 , -1 , 0, getOptions(), readPref, getDecoder() );
        
        DBObject obj = (i.hasNext() ? i.next() : null);
        if ( obj != null && ( fields != null && fields.keySet().size() > 0 ) ){
            obj.markAsPartialObject();
        }
        return obj;
    }

    // Only create a new decoder if there is a decoder factory explicitly set on the collection.  Otherwise return null
    // so that DBPort will use a cached decoder from the default factory.
    private DBDecoder getDecoder() {
        return getDBDecoderFactory() != null ? getDBDecoderFactory().create() : null;
    }

    // Only create a new encoder if there is an encoder factory explicitly set on the collection.  Otherwise return null
    // to allow DB to create its own or use a cached one.
    private DBEncoder getDBEncoder() {
        return getDBEncoderFactory() != null ? getDBEncoderFactory().create() : null;
    }


    /**
     * calls {@link DBCollection#apply(com.mongodb.DBObject, boolean)} with ensureID=true
     * @param o <code>DBObject</code> to which to add fields
     * @return the modified parameter object
     */
    public Object apply( DBObject o ){
        return apply( o , true );
    }

    /**
     * calls {@link DBCollection#doapply(com.mongodb.DBObject)}, optionally adding an automatic _id field
     * @param jo object to add fields to
     * @param ensureID whether to add an <code>_id</code> field
     * @return the modified object <code>o</code>
     */
    public Object apply( DBObject jo , boolean ensureID ){

        Object id = jo.get( "_id" );
        if ( ensureID && id == null ){
            id = ObjectId.get();
            jo.put( "_id" , id );
        }

        doapply( jo );

        return id;
    }

    /**
     * calls {@link DBCollection#save(com.mongodb.DBObject, com.mongodb.WriteConcern)} with default WriteConcern
     * @param jo the <code>DBObject</code> to save
     *        will add <code>_id</code> field to jo if needed
     * @return
     * @throws MongoException
     */
    public WriteResult save( DBObject jo ){
    	return save(jo, getWriteConcern());
    }

    /**
     * Saves an object to this collection (does insert or update based on the object _id).
     * @param jo the <code>DBObject</code> to save
     * @param concern the write concern
     * @return
     * @throws MongoException
     */
    public WriteResult save( DBObject jo, WriteConcern concern ){
        if ( checkReadOnly( true ) )
            return null;

        _checkObject( jo , false , false );

        Object id = jo.get( "_id" );

        if ( id == null || ( id instanceof ObjectId && ((ObjectId)id).isNew() ) ){
            if ( id != null && id instanceof ObjectId )
                ((ObjectId)id).notNew();
            if ( concern == null )
            	return insert( jo );
            else
            	return insert( jo, concern );
        }

        DBObject q = new BasicDBObject();
        q.put( "_id" , id );
        if ( concern == null )
        	return update( q , jo , true , false );
        else
        	return update( q , jo , true , false , concern );

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
     * returns the number of documents in this collection.
     * @return
     * @throws MongoException
     */
    public long count(){
        return getCount(new BasicDBObject(), null);
    }

    /**
     * returns the number of documents that match a query.
     * @param query query to match
     * @return
     * @throws MongoException
     */
    public long count(DBObject query){
        return getCount(query, null);
    }
    
    /**
     * returns the number of documents that match a query.
     * @param query query to match
     * @param readPrefs ReadPreferences for this query
     * @return
     * @throws MongoException
     */
    public long count(DBObject query, ReadPreference readPrefs ){
        return getCount(query, null, readPrefs);
    }


    /**
     *  calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject)} with an empty query and null fields.
     *  @return number of documents that match query
     *  @throws MongoException
     */
    public long getCount(){
        return getCount(new BasicDBObject(), null);
    }
    
    /**
     *  calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.ReadPreference)} with empty query and null fields.
     *  @param readPrefs ReadPreferences for this command
     *  @return number of documents that match query
     *  @throws MongoException
     */
    public long getCount(ReadPreference readPrefs){
        return getCount(new BasicDBObject(), null, readPrefs);
    }

    /**
     *  calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject)} with null fields.
     *  @param query query to match
     *  @return
     *  @throws MongoException
     */ 
    public long getCount(DBObject query){
        return getCount(query, null);
    }

    
    /**
     *  calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, long, long)} with limit=0 and skip=0
     *  @param query query to match
     *  @param fields fields to return
     *  @return
     *  @throws MongoException
     */
    public long getCount(DBObject query, DBObject fields){
        return getCount( query , fields , 0 , 0 );
    }
    
    /**
     *  calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, long, long, com.mongodb.ReadPreference)} with limit=0 and skip=0
     *  @param query query to match
     *  @param fields fields to return
     *  @param readPrefs ReadPreferences for this command
     *  @return
     *  @throws MongoException
     */
    public long getCount(DBObject query, DBObject fields, ReadPreference readPrefs){
        return getCount( query , fields , 0 , 0, readPrefs );
    }

    /**
     *  calls {@link DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject, long, long, com.mongodb.ReadPreference)} with the DBCollection's ReadPreference
     *  @param query query to match
     *  @param fields fields to return
     *  @param limit limit the count to this value
     *  @param skip skip number of entries to skip
     *  @return
     *  @throws MongoException
     */
    public long getCount(DBObject query, DBObject fields, long limit, long skip){
    	return getCount(query, fields, limit, skip, getReadPreference());
    }
    
    /**
     *  Returns the number of documents in the collection
     *  that match the specified query
     *
     *  @param query query to select documents to count
     *  @param fields fields to return
     *  @param limit limit the count to this value
     *  @param skip number of entries to skip
     *  @param readPrefs ReadPreferences for this command
     *  @return number of documents that match query and fields
     *  @throws MongoException
     */

    public long getCount(DBObject query, DBObject fields, long limit, long skip, ReadPreference readPrefs ){
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

        CommandResult res = _db.command(cmd,getOptions(),readPrefs);
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
     * renames of this collection to newName
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
     * calls {@link DBCollection#group(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, java.lang.String, java.lang.String)} with finalize=null
     * @param key - { a : true }
     * @param cond - optional condition on query
     * @param reduce javascript reduce function
     * @param initial initial value for first match on a key
     * @return
     * @throws MongoException
     * @see <a href="http://www.mongodb.org/display/DOCS/Aggregation">http://www.mongodb.org/display/DOCS/Aggregation</a>
     */
    public DBObject group( DBObject key , DBObject cond , DBObject initial , String reduce ){
        return group( key , cond , initial , reduce , null );
    }    
    
    /**
     * Applies a group operation
     * @param key - { a : true }
     * @param cond - optional condition on query
     * @param reduce javascript reduce function
     * @param initial initial value for first match on a key
     * @param finalize An optional function that can operate on the result(s) of the reduce function.
     * @return
     * @throws MongoException
     * @see <a href="http://www.mongodb.org/display/DOCS/Aggregation">http://www.mongodb.org/display/DOCS/Aggregation</a>
     */
    public DBObject group( DBObject key , DBObject cond , DBObject initial , String reduce , String finalize ){
        GroupCommand cmd = new GroupCommand(this, key, cond, initial, reduce, finalize);
        return group( cmd );
    }
    
    /**
     * Applies a group operation
     * @param key - { a : true }
     * @param cond - optional condition on query
     * @param reduce javascript reduce function
     * @param initial initial value for first match on a key
     * @param finalize An optional function that can operate on the result(s) of the reduce function.
     * @param readPrefs ReadPreferences for this command
     * @return
     * @throws MongoException
     * @see <a href="http://www.mongodb.org/display/DOCS/Aggregation">http://www.mongodb.org/display/DOCS/Aggregation</a>
     */
    public DBObject group( DBObject key , DBObject cond , DBObject initial , String reduce , String finalize, ReadPreference readPrefs ){
        GroupCommand cmd = new GroupCommand(this, key, cond, initial, reduce, finalize);
        return group( cmd, readPrefs );
    }

    /**
     * Applies a group operation
     * @param cmd the group command
     * @return
     * @throws MongoException
     * @see <a href="http://www.mongodb.org/display/DOCS/Aggregation">http://www.mongodb.org/display/DOCS/Aggregation</a>
     */
    public DBObject group( GroupCommand cmd ) {
        return group(cmd, getReadPreference());
    }

    /**
     * Applies a group operation
     * @param cmd the group command
     * @param readPrefs ReadPreferences for this command
     * @return
     * @throws MongoException
     * @see <a href="http://www.mongodb.org/display/DOCS/Aggregation">http://www.mongodb.org/display/DOCS/Aggregation</a>
     */
    public DBObject group( GroupCommand cmd, ReadPreference readPrefs ) {
        CommandResult res =  _db.command( cmd.toDBObject(), getOptions(), readPrefs );
        res.throwOnError();
        return (DBObject)res.get( "retval" );
    }

    /**
     * @deprecated prefer the {@link DBCollection#group(com.mongodb.GroupCommand)} which is more standard
     * Applies a group operation
     * @param args object representing the arguments to the group function
     * @return
     * @throws MongoException
     * @see <a href="http://www.mongodb.org/display/DOCS/Aggregation">http://www.mongodb.org/display/DOCS/Aggregation</a>
     */
    @Deprecated
    public DBObject group( DBObject args ){
        args.put( "ns" , getName() );
        CommandResult res =  _db.command( new BasicDBObject( "group" , args ), getOptions(), getReadPreference() );
        res.throwOnError();
        return (DBObject)res.get( "retval" );
    }

    /**
     * find distinct values for a key
     * @param key
     * @return
     * @throws MongoException
     */
    public List distinct( String key ){
        return distinct( key , new BasicDBObject() );
    }
    
    /**
     * find distinct values for a key
     * @param key
     * @param readPrefs
     * @return
     * @throws MongoException
     */
    public List distinct( String key, ReadPreference readPrefs ){
        return distinct( key , new BasicDBObject(), readPrefs );
    }

    /**
     * find distinct values for a key
     * @param key
     * @param query query to match
     * @return
     * @throws MongoException
     */
     public List distinct( String key , DBObject query ){
         return distinct(key, query, getReadPreference());
     }

    /**
     * find distinct values for a key
     * @param key
     * @param query query to match
     * @param readPrefs
     * @return
     * @throws MongoException
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
     * performs a map reduce operation
     * Runs the command in REPLACE output mode (saves to named collection)
     *
     * @param map
     *            map function in javascript code
     * @param outputTarget
     *            optional - leave null if want to use temp collection
     * @param reduce
     *            reduce function in javascript code
     * @param query
     *            to match
     * @return
     * @throws MongoException
     * @dochub mapreduce
     */
    public MapReduceOutput mapReduce( String map , String reduce , String outputTarget , DBObject query ){
        return mapReduce( new MapReduceCommand( this , map , reduce , outputTarget , MapReduceCommand.OutputType.REPLACE, query ) );
    }

    /**
     * performs a map reduce operation
     * Specify an outputType to control job execution
     * * INLINE - Return results inline
     * * REPLACE - Replace the output collection with the job output
     * * MERGE - Merge the job output with the existing contents of outputTarget
     * * REDUCE - Reduce the job output with the existing contents of
     * outputTarget
     *
     * @param map
     *            map function in javascript code
     * @param outputTarget
     *            optional - leave null if want to use temp collection
     * @param outputType
     *            set the type of job output
     * @param reduce
     *            reduce function in javascript code
     * @param query
     *            to match
     * @return
     * @throws MongoException
     * @dochub mapreduce
     */
    public MapReduceOutput mapReduce( String map , String reduce , String outputTarget , MapReduceCommand.OutputType outputType , DBObject query ){
        return mapReduce( new MapReduceCommand( this , map , reduce , outputTarget , outputType , query ) );
    }

    /**
     * performs a map reduce operation
     * Specify an outputType to control job execution
     * * INLINE - Return results inline
     * * REPLACE - Replace the output collection with the job output
     * * MERGE - Merge the job output with the existing contents of outputTarget
     * * REDUCE - Reduce the job output with the existing contents of
     * outputTarget
     *
     * @param map
     *            map function in javascript code
     * @param outputTarget
     *            optional - leave null if want to use temp collection
     * @param outputType
     *            set the type of job output
     * @param reduce
     *            reduce function in javascript code
     * @param query
     *            to match
     * @param readPrefs
     *            ReadPreferences for this operation
     * @return
     * @throws MongoException
     * @dochub mapreduce
     */
    public MapReduceOutput mapReduce( String map , String reduce , String outputTarget , MapReduceCommand.OutputType outputType , DBObject query, ReadPreference readPrefs ){
        MapReduceCommand command = new MapReduceCommand( this , map , reduce , outputTarget , outputType , query );
        command.setReadPreference(readPrefs);
        return mapReduce( command );
    }

    /**
     * performs a map reduce operation
     *
     * @param command
     *            object representing the parameters
     * @return
     * @throws MongoException
     */
    public MapReduceOutput mapReduce( MapReduceCommand command ){
        DBObject cmd = command.toDBObject();
        // if type in inline, then query options like slaveOk is fine
        CommandResult res = _db.command( cmd, getOptions(), command.getReadPreference() != null ? command.getReadPreference() : getReadPreference() );
        res.throwOnError();
        return new MapReduceOutput( this , cmd, res );
    }

    /**
     * performs a map reduce operation
     *
     * @param command
     *            object representing the parameters
     * @return
     * @throws MongoException
     */
    public MapReduceOutput mapReduce( DBObject command ){
        if ( command.get( "mapreduce" ) == null && command.get( "mapReduce" ) == null )
            throw new IllegalArgumentException( "need mapreduce arg" );
        CommandResult res = _db.command( command, getOptions(), getReadPreference() );
        res.throwOnError();
        return new MapReduceOutput( this , command, res );
    }
    
    /**
     * performs an aggregation operation
     *
     * @param firstOp
     *          requisite first operation to be performed in the aggregation pipeline
     *            
     * @param additionalOps
     *          additional operations to be performed in the aggregation pipeline
     * @return The aggregation operation's result set
     * 
     */
    public AggregationOutput aggregate( DBObject firstOp, DBObject ... additionalOps){
        if (firstOp == null)
            throw new IllegalArgumentException("aggregate can not accept null pipeline operation");
        
        DBObject command = new BasicDBObject("aggregate", _name );
        
        List<DBObject> pipelineOps = new ArrayList<DBObject>();
        pipelineOps.add(firstOp);
        Collections.addAll(pipelineOps, additionalOps);
        command.put( "pipeline", pipelineOps );
        
        CommandResult res = _db.command( command, getOptions(), getReadPreference() );
        res.throwOnError();
        return new AggregationOutput( command, res );
    }

    /**
     *   Return a list of the indexes for this collection.  Each object
     *   in the list is the "info document" from MongoDB
     *
     *   @return list of index documents
     *   @throws MongoException
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
     * gets the collections statistics ("collstats" command)
     * @return
     * @throws MongoException
     */
    public CommandResult getStats() {
        return getDB().command(new BasicDBObject("collstats", getName()), getOptions(), getReadPreference());
    }

    /**
     * returns whether or not this is a capped collection
     * @return
     * @throws MongoException
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
     * Finds a collection that is prefixed with this collection's name.
     * A typical use of this might be
     * <blockquote><pre>
     *    DBCollection users = mongo.getCollection( "wiki" ).getCollection( "users" );
     * </pre></blockquote>
     * Which is equivalent to
     * <pre><blockquote>
     *   DBCollection users = mongo.getCollection( "wiki.users" );
     * </pre></blockquote>
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
     * @param strict if an exception should be thrown if the database is read-only
     * @return if this collection's database is read-only
     * @throws RuntimeException if the database is read-only and <code>strict</code> is set
     *
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
     * @throws IllegalArgumentException if <code>c</code> is not a DBObject
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
     * @return the class
     */
    public Class getObjectClass(){
        return _objectClass;
    }

    /**
     * sets the internal class
     * @param path
     * @param c
     */
    public void setInternalClass( String path , Class c ){
        _internalClass.put( path , c );
    }

    /**
     * gets the internal class
     * @param path
     * @return
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
     * Get the write concern for this collection.
     * @return
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
     * Gets the read preference
     * @return
     */
    public ReadPreference getReadPreference(){
        if ( _readPref != null )
            return _readPref;
        return _db.getReadPreference();
    }
    /**
     * makes this query ok to run on a slave node
     *
     * @deprecated Replaced with {@code ReadPreference.secondaryPreferred()}
     * @see com.mongodb.ReadPreference#secondaryPreferred()
     */
    @Deprecated
    public void slaveOk(){
        addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    /**
     * adds a default query option
     * @param option
     */
    public void addOption( int option ){
        _options.add(option);
    }

    /**
     * sets the default query options
     * @param options
     */
    public void setOptions( int options ){
        _options.set(options);
    }

    /**
     * resets the default query options
     */
    public void resetOptions(){
        _options.reset();
    }

    /**
     * gets the default query options
     * @return
     */
    public int getOptions(){
        return _options.get();
    }

    /**
     * Set a customer decoder factory for this collection.  Set to null to use the default from MongoOptions.
     * @param fact  the factory to set.
     */
    public synchronized void setDBDecoderFactory(DBDecoderFactory fact) {
        _decoderFactory = fact;
    }

    /**
     * Get the decoder factory for this collection.  A null return value means that the default from MongoOptions
     * is being used.
     * @return  the factory
     */
    public synchronized DBDecoderFactory getDBDecoderFactory() {
        return _decoderFactory;
    }

    /**
     * Set a customer encoder factory for this collection.  Set to null to use the default from MongoOptions.
     * @param fact  the factory to set.
     */
    public synchronized void setDBEncoderFactory(DBEncoderFactory fact) {
        _encoderFactory = fact;
    }

    /**
     * Get the encoder factory for this collection.  A null return value means that the default from MongoOptions
     * is being used.
     * @return  the factory
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

    final private Set<String> _createdIndexes = new HashSet<String>();
}
