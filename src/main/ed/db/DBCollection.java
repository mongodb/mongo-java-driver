// DBCollection.java

package ed.db;

import java.util.*;
import java.lang.reflect.*;

import ed.util.*;

/** DB Collection
 * 
 * Class for database collection objects.  When you invoke something like:
 *   var my_coll = db.students;
 * you get back a collection which can be used to perform various database operations.
 */
public abstract class DBCollection {

    /** @unexpose */
    final static boolean DEBUG = Boolean.getBoolean( "DEBUG.DB" );

    /** Saves an object to the database.
     * @param o object to save
     * @return the new database object
     */
    protected abstract DBObject doSave( DBObject o );

    /** Performs an update operation.
     * @param q search query for old object to update
     * @param o object with which to update <tt>q</tt>
     * @param upsert if the database should create the element if it does not exist
     * @param apply if an _id field should be added to the new object
     * See www.10gen.com/wiki/db.update
     */
    public abstract DBObject update( DBObject q , DBObject o , boolean upsert , boolean apply );

    /** Adds any necessary fields to a given object before saving it to the collection.
     * @param o object to which to add the fields
     */
    protected abstract void doapply( DBObject o );

    /** Removes an object from the database collection.
     * @return -1
     */
    public abstract int remove( DBObject o );

    /** Finds an object by its id.
     * @param id the id of the object
     * @return the object, if found
     */
    protected abstract DBObject dofind( ObjectId id );

    /** Finds an object.
     * @param ref query used to search
     * @param fields the fields of matching objects to return
     * @param numToSkip will not return the first <tt>numToSkip</tt> matches
     * @param numToReturn limit the results to this number
     * @return the objects, if found
     */
    public abstract Iterator<DBObject> find( DBObject ref , DBObject fields , int numToSkip , int numToReturn );

    /** Ensures an index on this collection (that is, the index will be created if it does not exist).
     * ensureIndex is optimized and is inexpensive if the index already exists.
     * @param keys fields to use for index
     * @param name an identifier for the index
     */
    public abstract void ensureIndex( DBObject keys , String name );

    // ------

    /** Finds an object by its id.
     * @param id the id of the object
     * @return the object, if found
     */
    public final DBObject find( ObjectId id ){
        ensureIDIndex();

        DBObject ret = dofind( id );

        if ( ret == null )
            return null;

        apply( ret , false );

        return ret;
    }

    public final DBObject find( String id ){
        if ( ! ObjectId.isValid( id ) )
            throw new IllegalArgumentException( "invalid object id [" + id + "]" );
        return find( new ObjectId( id ) );
    }

    /** Ensures an index on the id field, if one does not already exist.
     * @param key an object with an _id field.
     */
    public void checkForIDIndex( DBObject key ){
        if ( _checkedIdIndex ) // we already created it, so who cares
            return;

        if ( key.get( "_id" ) == null )
            return;

        if ( key.keySet().size() > 1 )
            return;

        ensureIDIndex();
    }

    /** Creates an index on the id field, if one does not already exist.
     * @param key an object with an _id field.
     */
    public void ensureIDIndex(){
        if ( _checkedIdIndex )
            return;

        ensureIndex( _idKey );
        _checkedIdIndex = true;
    }

    /** Creates an index on a set of fields, if one does not already exist.
     * @param keys an object with a key set of the fields desired for the index
     */
    public final void ensureIndex( final DBObject keys ){
        ensureIndex( keys , false );
    }

    /** Forces creation of an index on a set of fields, if one does not already exist.
     * @param keys an object with a key set of the fields desired for the index
     */
    public final void createIndex( final DBObject keys ){
        ensureIndex( keys , true );
    }

    /** Creates an index on a set of fields, if one does not already exist.
     * @param keys an object with a key set of the fields desired for the index
     * @param force if index creation should be forced, even if it is unnecessary
     */
    public final void ensureIndex( final DBObject keys , final boolean force ){
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

        ensureIndex( keys , name );

        _createIndexes.add( name );
        if ( _anyUpdateSave )
            _createIndexesAfterSave.add( name );
    }

    /** Clear all indices on this collection. */
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

    public void setHintFields( List<DBObject> lst ){
        _hintFields = lst;
    }

    /** Queries for an object in this collection.
     * @param ref object for which to search
     * @return an iterator over the results
     */
    public final Iterator<DBObject> find( DBObject ref ){
        return find( ref == null ? new BasicDBObject() : ref , null , 0 , 0 );
    }

    public final Iterator<DBObject> find(){
        Iterator<DBObject> i = find( new BasicDBObject() , null , 0 , 0 );
        if ( i == null )
            return (new LinkedList<DBObject>()).iterator();
        return i;
    }

    public final DBObject findOne(){
        return findOne( new BasicDBObject() );
    }

    public final DBObject findOne( DBObject o ){
        Iterator<DBObject> i = find( o , null , 0 , 1 );
        if ( i == null || ! i.hasNext() )
            return null;
        return i.next();
    }

    /**
     */
    public final ObjectId apply( DBObject o ){
        return apply( o , true );
    }
    
    /** Adds the "private" fields _save, _update, and _id to an object.
     * @param o object to which to add fields
     * @param ensureID if an _id field is needed
     * @return the _id assigned to the object
     * @throws RuntimeException if <tt>o</tt> is not a DBObject
     */
    public final ObjectId apply( DBObject jo , boolean ensureID ){

        ObjectId id = (ObjectId)jo.get( "_id" );
        if ( ensureID && id == null ){
            id = ObjectId.get();
            jo.put( "_id" , id );
        }

        doapply( jo );

        return id;
    }

    /** Saves an object to this collection executing the preSave function in a given scope.
     * @param s scope to use (can be null)
     * @param o the object to save
     * @return the new object from the collection
     */
    public final DBObject save( DBObject jo ){
        if ( checkReadOnly( true ) ) 
            return jo;

        _checkObject( jo , false , false );
        
        //_findSubObject( s , jo , null );

        ObjectId id = null;
	{ 
	    Object temp = jo.get( "_id" );
	    if ( temp != null && ! ( temp instanceof ObjectId ) )
		throw new RuntimeException( "_id has to be an ObjectId" );
	    id = (ObjectId)temp;
	}
        if ( DEBUG ) System.out.println( "id : " + id );

        if ( id == null || id._new ){
            if ( DEBUG ) System.out.println( "saving new object" );
            if ( id != null )
                id._new = false;
            doSave( jo );
            return jo;
        }

        if ( DEBUG ) System.out.println( "doing implicit upsert : " + jo.get( "_id" ) );
        DBObject q = new BasicDBObject();
        q.put( "_id" , id );
        return update( q , jo , true , false );
    }

    // ---- DB COMMANDS ----

    public void dropIndexes(){
        BasicDBObject res = (BasicDBObject)_base.command( BasicDBObjectBuilder.start().add( "deleteIndexes" , getName() ).add( "index" , "*" ).get() );
        if ( res.getInt( "ok" , 0 ) != 1 )
            throw new RuntimeException( "error dropping indexes : " + res );

        resetIndexCache();
    }
    
    public void drop(){
        dropIndexes();
        BasicDBObject res = (BasicDBObject)_base.command( BasicDBObjectBuilder.start().add( "drop" , getName() ).get() );
        if ( res.getInt( "ok" , 0 ) != 1 )
            throw new RuntimeException( "error dropping : " + res );
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

    private final DBObject _checkObject( DBObject o , boolean canBeNull , boolean query ){
        if ( o == null ){
            if ( canBeNull )
                return null;
            throw new NullPointerException( "can't be null" );
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
     * @return this collection's database
     */
    public DBBase getDB(){
        return _base;
    }

    /** Returns the database this collection is a member of.
     * @return this collection's database
     */
    public DBBase getBase(){
        return _base;
    }

    /** Returns if this collection can be modified.
     * @return if this collection can be modified
     */
    protected boolean checkReadOnly( boolean strict ){
        if ( ! _base._readOnly )
            return false;

        if ( ! strict )
            return true;

        throw new RuntimeException( "db is read only" );
    }

    /** Calculates the hash code for this collection.
     * @return the hash code
     */
    public int hashCode(){
        return _fullName.hashCode();
    }

    /** Checks if this collection is equal to another object.
     * @param o object with which to compare this collection
     * @return if the two collections are equal
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

    final DBBase _base;

    final protected String _name;
    final protected String _fullName;

    protected List<DBObject> _hintFields;

    private boolean _anyUpdateSave = false;

    private boolean _checkedIdIndex = false;
    final private Set<String> _createIndexes = new HashSet<String>();
    final private Set<String> _createIndexesAfterSave = new HashSet<String>();

    private final static DBObject _upsertOptions = BasicDBObjectBuilder.start().add( "upsert" , true ).get();
    private final static DBObject _idKey = BasicDBObjectBuilder.start().add( "_id" , ObjectId.get() ).get();

}
