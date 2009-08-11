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

import java.util.*;

/** This class provides a skeleton implementation of a database collection.  
 * <p>A typical invocation sequence is thus
 * <blockquote><pre>
 *     Mongo mongo = new Mongo( new DBAddress( "localhost", 127017 ) );
 *     DBCollection collection = mongo.getCollection( "test" );
 * </pre></blockquote>
 */
public abstract class DBCollection {

    final static boolean DEBUG = Boolean.getBoolean( "DEBUG.DB" );

    /**
     * Saves an document to the database.
     * @param doc object to save
     * @return the new database object
     */
    public abstract DBObject insert(DBObject doc) throws MongoException;

    /**
     * Saves an array of documents to the database.
     *
     * @param arr  array of documents to save
     * @return the new database object
     */
    public abstract DBObject[] insert(DBObject[] arr) throws MongoException;

    /**
     * Saves an array of documents to the database.
     *
     * @param list  list of documents to save
     * @return the new database object
     */
    public abstract List<DBObject> insert(List<DBObject> list) throws MongoException;

    /**
     * Performs an update operation.
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @param upsert if the database should create the element if it does not exist
     * @param apply if an _id field should be added to the new object
     * See http://www.mongodb.org/display/DOCS/Atomic+Operations
     */
    public abstract DBObject update( DBObject q , DBObject o , boolean upsert , boolean apply ) throws MongoException ;

    /** Adds any necessary fields to a given object before saving it to the collection.
     * @param o object to which to add the fields
     */
    protected abstract void doapply( DBObject o );

    /** Removes objects from the database collection.
     * @param the object that documents to be removed must match
     */
    public abstract void remove( DBObject o ) throws MongoException ;

    /** Finds an object.
     * @param ref query used to search
     * @param fields the fields of matching objects to return
     * @param numToSkip will not return the first <tt>numToSkip</tt> matches
     * @param numToReturn limit the results to this number
     * @return the objects, if found
     */
    abstract Iterator<DBObject> find( DBObject ref , DBObject fields , int numToSkip , int numToReturn ) throws MongoException ;

    /** Ensures an index on this collection (that is, the index will be created if it does not exist).
     * ensureIndex is optimized and is inexpensive if the index already exists.
     * @param keys fields to use for index
     * @param name an identifier for the index
     */
    public abstract void ensureIndex( DBObject keys , String name ) throws MongoException ;

    /** Ensures an optionally unique index on this collection.
     * @param keys fields to use for index
     * @param name an identifier for the index
     * @param unique if the index should be unique
     */
    public abstract void ensureIndex( DBObject keys , String name , boolean unique ) throws MongoException ;

    // ------

    /**
     *   Finds an object by its id.  This compares the passed in value to the _id field of the document
     * 
     * @param obj any valid object
     * @return the object, if found, otherwise <code>null</code>
     */
    public final DBObject findOne( Object obj ) 
        throws MongoException {
        ensureIDIndex();

        Iterator<DBObject> iterator =  find(new BasicDBObject("_id", obj), null, 0,1);

        return (iterator != null ? iterator.next() : null);
    }

    /** Ensures an index on the id field, if one does not already exist.
     * @param key an object with an _id field.
     */
    public void checkForIDIndex( DBObject key )
        throws MongoException {
        if ( _checkedIdIndex ) // we already created it, so who cares
            return;

        if ( key.get( "_id" ) == null )
            return;

        if ( key.keySet().size() > 1 )
            return;

        ensureIDIndex();
    }

    /** Creates an index on the id field, if one does not already exist.
     */
    public void ensureIDIndex()
        throws MongoException {
        if ( _checkedIdIndex )
            return;

        ensureIndex( _idKey );
        _checkedIdIndex = true;
    }

    /** Creates an index on a set of fields, if one does not already exist.
     * @param keys an object with a key set of the fields desired for the index
     */
    public final void ensureIndex( final DBObject keys )
        throws MongoException {
        ensureIndex( keys , false );
    }

    /** Forces creation of an index on a set of fields, if one does not already exist.
     * @param keys an object with a key set of the fields desired for the index
     */
    public final void createIndex( final DBObject keys )
        throws MongoException {
        ensureIndex( keys , true );
    }

    /** Creates an index on a set of fields, if one does not already exist.
     * @param keys an object with a key set of the fields desired for the index
     * @param force if index creation should be forced, even if it is unnecessary
     */
    public final void ensureIndex( final DBObject keys , final boolean force )
        throws MongoException {
        ensureIndex( keys , force, false );
    }

    public final void ensureIndex( final DBObject keys , final boolean force , final boolean unique )
        throws MongoException {

        if ( checkReadOnly( false ) ) return;

        final String name = genIndexName( keys );

        boolean doEnsureIndex = false;
        if ( Math.random() > 0.999 )
            doEnsureIndex = true;
        else if ( ! _createIndexes.contains( name ) )
            doEnsureIndex = true;
        else if ( _anyUpdateSave && ! _createIndexesAfterSave.contains( name ) )
            doEnsureIndex = true;

        if ( ! ( force || doEnsureIndex ) )
            return;

        ensureIndex( keys , name , unique );

        _createIndexes.add( name );
        if ( _anyUpdateSave )
            _createIndexesAfterSave.add( name );
    }

    /** Clears all indices that have not yet been applied to this collection. */
    public void resetIndexCache(){
        _createIndexes.clear();
    }

    /** Generate an index name from the set of fields it is over.
     * @param keys the names of the fields used in this index
     * @return a string representation of this index's fields
     */
    public static String genIndexName( DBObject keys ){
        String name = "";
        for ( String s : keys.keySet() ){
            if ( name.length() > 0 )
                name += "_";
            name += s + "_";
            Object val = keys.get( s );
            if ( val instanceof Number )
                name += val.toString().replace( ' ' , '_' );
        }
        return name;
    }

    /** Set hint fields for this collection.
     * @param lst a list of <code>DBObject</code>s to be used as hints
     */
    public void setHintFields( List<DBObject> lst ){
        _hintFields = lst;
    }

    /** Queries for an object in this collection.
     * @param ref object for which to search
     * @return an iterator over the results
     */
    public final DBCursor find( DBObject ref ){
        return new DBCursor( this, ref, null );
    }

    /** Queries for an object in this collection.
     * @param ref object for which to search
     * @return a cursor which will iterate over every object
     */
    public final DBCursor find( DBObject ref , DBObject keys ){
        return new DBCursor( this, ref, keys );
    }

    /** Queries for all objects in this collection. 
     * @return a cursor which will iterate over every object
     */
    public final DBCursor find(){
        return new DBCursor( this, new BasicDBObject(), null );
    }

    /** Returns a single object from this collection.
     * @return the object found, or <code>null</code> if the collection is empty
     */
    public final DBObject findOne()
        throws MongoException {
        return findOne( new BasicDBObject() );
    }

    /** Returns a single object from this collection matching the query.
     * @param o the query object
     * @return the object found, or <code>null</code> if no such object exists
     */
    public final DBObject findOne( DBObject o )
        throws MongoException {
        Iterator<DBObject> i = find( o , null , 0 , 1 );
        if ( i == null || ! i.hasNext() )
            return null;
        return i.next();
    }

    /** Adds the "private" fields _save, _update, and _id to an object.
     * @param o <code>DBObject</code> to which to add fields
     * @return the modified parameter object
     */
    public final Object apply( DBObject o ){
        return apply( o , true );
    }
    
    /** Adds the "private" fields _save, _update, and (optionally) _id to an object.
     * @param jo object to which to add fields
     * @param ensureID whether to add an <code>_id</code> field or not
     * @return the modified object <code>o</code>
     */
    public final Object apply( DBObject jo , boolean ensureID ){
        
        Object id = jo.get( "_id" );
        if ( ensureID && id == null ){
            id = ObjectId.get();
            jo.put( "_id" , id );
        }

        doapply( jo );

        return id;
    }

    /** Saves an object to this collection.
     * @param jo the <code>DBObject</code> to save
     * @return <code>jo</code> with <code>_id</code> field added, if needed
     */
    public final DBObject save( DBObject jo )
        throws MongoException {
        if ( checkReadOnly( true ) ) 
            return jo;

        _checkObject( jo , false , false );
        
        //_findSubObject( s , jo , null );

        Object id = jo.get( "_id" );
        if ( DEBUG ) System.out.println( "id : " + id );

        if ( id == null || ( id instanceof ObjectId && ((ObjectId)id)._new ) ){
            if ( DEBUG ) System.out.println( "saving new object" );
            if ( id != null && id instanceof ObjectId )
                ((ObjectId)id)._new = false;
            insert( jo );
            return jo;
        }

        if ( DEBUG ) System.out.println( "doing implicit upsert : " + jo.get( "_id" ) );
        DBObject q = new BasicDBObject();
        q.put( "_id" , id );
        return update( q , jo , true , false );
    }
    
    // ---- DB COMMANDS ----
    /** Drops all indices from this collection
     */
    public void dropIndexes()
        throws MongoException {
        BasicDBObject res = (BasicDBObject)_base.command( BasicDBObjectBuilder.start().add( "deleteIndexes" , getName() ).add( "index" , "*" ).get() );
        if ( res.getInt( "ok" , 0 ) != 1 ){
            if ( res.getString( "errmsg" ).equals( "ns not found" ) )
                return;
            throw new MongoException( "error dropping indexes : " + res );
        }
        
        resetIndexCache();
    }
    
    /** Drops (deletes) this collection
     */
    public void drop()
        throws MongoException {
        BasicDBObject res = (BasicDBObject)_base.command( BasicDBObjectBuilder.start().add( "drop" , getName() ).get() );
        if ( res.getInt( "ok" , 0 ) != 1 ){
            if ( res.getString( "errmsg" ).equals( "ns not found" ) )
                return;
            throw new MongoException( "error dropping : " + res );
        }
    }

    /**
     *  Returns the number of documents in the collection
     *  @return number of documents that match query
     */
    public long getCount()
        throws MongoException {
        return getCount(new BasicDBObject(), null);
    }

    /**
     *  Returns the number of documents in the collection
     *  that match the specified query
     *
     *  @param query query to select documents to count
     *  @return number of documents that match query
     */
    public long getCount(DBObject query)
        throws MongoException {
        return getCount(query, null);
    }

    /**
     *  Returns the number of documents in the collection
     *  that match the specified query
     *
     *  @param query query to select documents to count
     *  @param fields fields to return
     *  @return number of documents that match query and fields
     */
    public long getCount(DBObject query, DBObject fields)
        throws MongoException {

        BasicDBObject cmd = new BasicDBObject();
        cmd.put("count", getName());
        cmd.put("query", query);
        if (fields != null) {
            cmd.put("fields", fields);
        }

        BasicDBObject res = (BasicDBObject)_base.command(cmd);

        if (res.getInt("ok" , 0 ) != 1 ){
            String errmsg = res.getString( "errmsg" );
            if ( errmsg.equals("ns does not exist") || 
                 errmsg.equals("ns missing" ) ){
                // for now, return 0 - lets pretend it does exist
                return 0;
            }
            throw new MongoException( "error counting : " + res );
        }

        return res.getLong("n");
    }

    /**
     *   Return a list of the indexes for this collection.  Each object
     *   in the list is the "info document" from MongoDB
     *
     *   @return list of index documents
     */
    public List<DBObject> getIndexInfo() {
        BasicDBObject cmd = new BasicDBObject();
        cmd.put("ns", getFullName());

        DBCursor cur = _base.getCollection("system.indexes").find(cmd);

        List<DBObject> list = new ArrayList<DBObject>();

        while(cur.hasNext()) {
            list.add(cur.next());
        }

        return list;
    }

    // ------

    /** Initializes a new collection.
     * @param base database in which to create the collection
     * @param name the name of the collection
     */
    protected DBCollection( DBBase base , String name ){
        _base = base;
        _name = name;
        _fullName = _base.getName() + "." + name;
    }

    private  DBObject _checkObject( DBObject o , boolean canBeNull , boolean query ){
        if ( o == null ){
            if ( canBeNull )
                return null;
            throw new IllegalArgumentException( "can't be null" );
        }

        if ( o.isPartialObject() && ! query )
            throw new IllegalArgumentException( "can't save partial objects" );
        
        if ( ! query ){
            for ( String s : o.keySet() ){
                if ( s.contains( "." ) )
                    throw new IllegalArgumentException( "fields stored in the db can't have . in them" );
                if ( s.contains( "$" ) )
                    throw new IllegalArgumentException( "fields stored in the db can't have $ in them" );
            }
        }
        return o;
    }
    /*
    private void _findSubObject( Scope scope , DBObject jo , IdentitySet seenSubs ){
        if ( seenSubs == null )
            seenSubs = new IdentitySet();

        if ( seenSubs.contains( jo ) )
            return;
        seenSubs.add( jo );
        

        if ( DEBUG ) System.out.println( "_findSubObject on : " + jo.get( "_id" ) );

        LinkedList<DBObject> toSearch = new LinkedList();
        Map<DBObject,String> seen = new IdentityHashMap<DBObject,String>();
        toSearch.add( jo );

        while ( toSearch.size() > 0 ){
            
            Map<DBObject,String> seenNow = new IdentityHashMap<DBObject,String>( seen );
            
            DBObject n = toSearch.remove(0);
            for ( String name : n.keySet() ){

                Object foo = Bytes.safeGet( n , name );
                
                if ( foo == null )
                    continue;
                
                if ( ! ( foo instanceof DBObject ) )
                    continue;
                
                if ( foo instanceof DBRef ){
                    DBRef ref = (DBRef)foo;
                    if ( ! ref.isDirty() )
                        continue;
                    foo = ref.getRealObject();
                }

                if ( foo instanceof JSFunction )
                    continue;

		if ( foo instanceof JSString 
                     || foo instanceof JSRegex
                     || foo instanceof JSDate )
		    continue;
                
                if ( foo instanceof DBCollection || 
                     foo instanceof DBBase )
                    continue;
                
                DBObject e = (DBObject)foo;
                if ( e instanceof BasicDBObject )
                    ((BasicDBObject)e).prefunc();

                if ( n.get( name ) == null )
                    continue;

                if ( e instanceof JSFileChunk ){
                    _base.getCollection( "_chunks" ).apply( e );
                }

                if ( e.get( "_ns" ) == null ){
                    if ( seen.containsKey( e ) )
                        throw new RuntimeException( "you have a loop. key : " + name + " from a " + n.getClass()  + " which is a : " + e.getClass() );
                    seenNow.put( e , "a" );
                    toSearch.add( e );
                    continue;
                }


                // ok - now we knows its a reference
                
                if ( e instanceof BasicDBObject &&
                     ((BasicDBObject)e).isPartialObject() ){
                    // TODO: i think this is correct
                    // this means you have a reference to an object that was retrieved with only some objects
                    // maybe this should do a extend, or throw an exception
                    // but certainly shouldn't overwrite
                    continue;
                }

                if ( e.get( "_id" ) == null ){ // new object, lets save it
                    JSFunction otherSave = e.getFunction( "_save" );
                    if ( otherSave == null )
                        throw new RuntimeException( "no save :(" );
                    otherSave.call( scope , e , null );
                    continue;
                }

                // old object, lets update TODO: dirty tracking
                DBObject lookup = new BasicDBObject();
                lookup.set( "_id" , e.get( "_id" ) );

                JSFunction otherUpdate = e.getFunction( "_update" );
                if ( otherUpdate == null ){

                    // already taken care of
                    if ( e instanceof DBRef )
                        continue;

                    throw new RuntimeException( "_update is null class: " + e.getClass().getName() + "  keyset : " + e.keySet() + " ns:" + e.get( "_ns" ) );
                }

                if ( e instanceof BasicDBObject && ! ((BasicDBObject)e).isDirty() )
                    continue;

                otherUpdate.call( scope , lookup , e , _upsertOptions , seenSubs );

            }
            
            seen.putAll( seenNow );
        }
    }
    */

    /** Find a collection that is prefixed with this collection's name.
     * A typical use of this might be 
     * <blockquote><pre>
     *    DBCollection users = mongo.getCollection( "wiki" ).getCollection( "users" );
     * </pre></blockquote>
     * Which is equilalent to
     * <pre><blockquote>
     *   DBCollection users = mongo.getCollection( "wiki.users" );
     * </pre></blockquote>
     * @param n the name of the collection to find
     * @return the matching collection
     */
    public DBCollection getCollection( String n ){
        return _base.getCollection( _name + "." + n );
    }

    /** Returns the name of this collection.
     * @return  the name of this collection
     */
    public String getName(){
        return _name;
    }

    /** Returns the full name of this collection, with the database name as a prefix.
     * @return  the name of this collection
     */
    public String getFullName(){
        return _fullName;
    }

    /** Returns the database this collection is a member of.
     * Same as <code>getBase()</code>.
     * @return this collection's database
     */
    public DBBase getDB(){
        return _base;
    }

    /** Returns the database this collection is a member of.
     * Same as <code>getBase()</code>.
     * @return this collection's database
     */
    public DBBase getBase(){
        return _base;
    }

    /** Returns if this collection's database is read-only
     * @param strict if an exception should be thrown if the database is read-only
     * @return if this collection's database is read-only
     * @throws RuntimeException if the database is read-only and <code>strict</code> is set
     */
    protected boolean checkReadOnly( boolean strict ){
        if ( ! _base._readOnly )
            return false;

        if ( ! strict )
            return true;

        throw new IllegalStateException( "db is read only" );
    }

    /** Calculates the hash code for this collection.
     * @return the hash code
     */
    public int hashCode(){
        return _fullName.hashCode();
    }

    /** Checks if this collection is equal to another object.
     * @param o object with which to compare this collection
     * @return if the two collections are the same object
     */
    public boolean equals( Object o ){
        return o == this;
    }

    /** Returns name of the collection.
     * @return name of the collection.
     */
    public String toString(){
        return _name;
    }

    /** Set a default class for objects in this collection
     * @param c the class
     * @throws IllegalArgumentException if <code>c</code> is not a DBObject
     */
    public void setObjectClass( Class c ){
        if ( ! DBObject.class.isAssignableFrom( c ) )
            throw new IllegalArgumentException( c.getName() + " is not a DBObject" );
        _objectClass = c;
        if ( ReflectionDBObject.class.isAssignableFrom( c ) )
            _wrapper = ReflectionDBObject.getWrapper( c );
        else 
            _wrapper = null;
    }
    
    /** Gets the default class for objects in the collection
     * @return the class
     */
    public Class getObjectClass(){
        return _objectClass;
    }

    public void setInternalClass( String path , Class c ){
        _internalClass.put( path , c );
    }

    protected Class getInternalClass( String path ){
        Class c = _internalClass.get( path );
        if ( c != null )
            return c;

        if ( _wrapper == null )
            return null;
        return _wrapper.getInternalClass( path );
    }
    
    final DBBase _base;

    final protected String _name;
    final protected String _fullName;

    protected List<DBObject> _hintFields;

    protected Class _objectClass = null;
    private Map<String,Class> _internalClass = Collections.synchronizedMap( new HashMap<String,Class>() );
    private ReflectionDBObject.JavaWrapper _wrapper = null;

    private boolean _anyUpdateSave = false;

    private boolean _checkedIdIndex = false;
    final private Set<String> _createIndexes = new HashSet<String>();
    final private Set<String> _createIndexesAfterSave = new HashSet<String>();

    private final static DBObject _upsertOptions = BasicDBObjectBuilder.start().add( "upsert" , true ).get();
    private final static DBObject _idKey = BasicDBObjectBuilder.start().add( "_id" , 1 ).get();

}
