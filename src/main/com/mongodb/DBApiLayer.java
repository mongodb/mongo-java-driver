// DBApiLayer.java

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

import java.nio.*;
import java.util.*;
import java.util.logging.*;

import com.mongodb.util.*;

import org.bson.*;
import org.bson.types.*;

/** Database API
 * This cannot be directly instantiated, but the functions are available
 * through instances of Mongo.
 */
public class DBApiLayer extends DB {

    static final boolean D = Boolean.getBoolean( "DEBUG.DB" );
    /** The maximum number of cursors allowed */
    static final int NUM_CURSORS_BEFORE_KILL = 100;

    static final boolean SHOW = Boolean.getBoolean( "DB.SHOW" );

    protected DBApiLayer( String root , DBConnector connector ){
        super( root );

        _root = root;
        _rootPlusDot = _root + ".";

        _connector = connector;
    }

    public void requestStart(){
        _connector.requestStart();
    }

    public void requestDone(){
        _connector.requestDone();
    }
    
    public void requestEnsureConnection(){
        _connector.requestEnsureConnection();
    }

    protected MyCollection doGetCollection( String name ){
        MyCollection c = _collections.get( name );
        if ( c != null )
            return c;

        synchronized ( _collections ){
            c = _collections.get( name );
            if ( c != null )
                return c;

            c = new MyCollection( name );
            _collections.put( name , c );
        }

        return c;
    }

    String _removeRoot( String ns ){
        if ( ! ns.startsWith( _rootPlusDot ) )
            return ns;
        return ns.substring( _root.length() + 1 );
    }


    /** Get a collection from a &lt;databaseName&gt;.&lt;collectionName&gt;.
     * If <code>fullNameSpace</code> does not contain any "."s, this will
     * find a collection called <code>fullNameSpace</code> and return it.
     * Otherwise, it will find the collecton <code>collectionName</code> and
     * return it.
     * @param fullNameSpace the full name to find
     * @throws RuntimeException if the database named is not this database
     */
    public DBCollection getCollectionFromFull( String fullNameSpace ){
        // TOOD security

        if ( fullNameSpace.indexOf( "." ) < 0 ) {
            // assuming local
            return doGetCollection( fullNameSpace );
        }

        final int idx = fullNameSpace.indexOf( "." );

        final String root = fullNameSpace.substring( 0 , idx );
        final String table = fullNameSpace.substring( idx + 1 );

        if (_root.equals(root)) {
            return doGetCollection( table );
        }
        
        return getSisterDB( root ).getCollection( table );
    }

    public DB getSisterDB( String dbName ){
        return new DBApiLayer( dbName , _connector );
    }

    class MyCollection extends DBCollection {
        MyCollection( String name ){
            super( DBApiLayer.this , name );
            _fullNameSpace = _root + "." + name;
        }

        public void doapply( DBObject o ){
        }

        public void insert( DBObject o )
            throws MongoException {
            insert( new DBObject[]{ o } );
        }

        public void insert(DBObject[] arr)
            throws MongoException {
            insert(arr, true);
        }

        public void insert(List<DBObject> list)
            throws MongoException {
            insert(list.toArray(new DBObject[list.size()]) , true);
        }

        protected void insert(DBObject obj, boolean shouldApply )
            throws MongoException {
            insert( new DBObject[]{ obj } , shouldApply );
        }

        protected void insert(DBObject[] arr, boolean shouldApply )
            throws MongoException {

            if ( SHOW ) {
                for (DBObject o : arr) {
                    System.out.println( "save:  " + _fullNameSpace + " " + JSON.serialize( o ) );
                }
            }
            
            if ( shouldApply ){
                for ( int i=0; i<arr.length; i++ ){
                    DBObject o=arr[i];
                    apply( o );
                    Object id = o.get( "_id" );
                    if ( id instanceof ObjectId ){
                        ((ObjectId)id).notNew();
                    }
                }
            }
            
            int cur = 0;
            while ( cur < arr.length ){
                OutMessage om = OutMessage.get( 2002 );
                
                om.writeInt( 0 ); // reserved
                om.writeCString( _fullNameSpace );
                
                for ( ; cur<arr.length; cur++ ){
                    DBObject o = arr[cur];
                    int sz = om.putObject( o );
                    if ( sz > Bytes.MAX_OBJECT_SIZE )
                        throw new IllegalArgumentException( "object too big: " + sz );
                    
                    if ( om.size() > ( 4 * 1024 * 1024 ) ){
                        cur++;
                        break;
                    }
                }
                
                _connector.say( _db , om , getWriteConcern() );
                
            }
        }
        
        public void remove( DBObject o )
            throws MongoException {

            if ( SHOW ) System.out.println( "remove: " + _fullNameSpace + " " + JSON.serialize( o ) );

            OutMessage om = OutMessage.get( 2006 );

            om.writeInt( 0 ); // reserved
            om.writeCString( _fullNameSpace );
            
            Collection<String> keys = o.keySet();

            if ( keys.size() == 1 &&
                 keys.iterator().next().equals( "_id" ) &&
                 o.get( keys.iterator().next() ) instanceof ObjectId )
                om.writeInt( 1 );
            else
                om.writeInt( 0 );

            om.putObject( o );
            
            _connector.say( _db , om , getWriteConcern() );
        }

        void _cleanCursors()
            throws MongoException {
            if ( _deadCursorIds.size() == 0 )
                return;

            if ( _deadCursorIds.size() % 20 != 0 && _deadCursorIds.size() < NUM_CURSORS_BEFORE_KILL )
                return;

            List<Long> l = _deadCursorIds;
            _deadCursorIds = new Vector<Long>();

            Bytes.LOGGER.info( "trying to kill cursors : " + l.size() );

            try {
                killCursors( l );
            }
            catch ( Throwable t ){
                Bytes.LOGGER.log( Level.WARNING , "can't clean cursors" , t );
                _deadCursorIds.addAll( l );
            }
        }

        void killCursors( List<Long> all )
            throws MongoException {
            if ( all == null || all.size() == 0 )
                return;

            OutMessage om = OutMessage.get( 2007 );
            om.writeInt( 0 ); // reserved
            
            om.writeInt( all.size() );

            for (Long l : all) {
                om.writeLong(l);
            }

            _connector.say( _db , om , WriteConcern.NONE );
        }

        public Iterator<DBObject> find( DBObject ref , DBObject fields , int numToSkip , int batchSize , int options )
            throws MongoException {
            
            if ( ref == null )
                ref = new BasicDBObject();
            
            if ( SHOW ) System.out.println( "find: " + _fullNameSpace + " " + JSON.serialize( ref ) );

            _cleanCursors();
            
            OutMessage query = OutMessage.get( 2004 );

            query.writeInt( options ); // options
            query.writeCString( _fullNameSpace );

            query.writeInt( numToSkip );
            query.writeInt( batchSize );
            query.putObject( ref ); // ref
            if ( fields != null )
                query.putObject( fields ); // fields to return
            
            ByteDecoder decoder = ByteDecoder.get( DBApiLayer.this , this );

            try {
                DBMessage response = _connector.call( _db , query , decoder , 2 );
                
                SingleResult res = new SingleResult( _fullNameSpace , decoder , options );

                if ( res._lst.size() == 0 )
                    return null;

                if ( res._lst.size() == 1 ){
                    Object err = res._lst.get(0).get( "$err" );
                    if ( err != null )
                        throw new RuntimeException( "db error [" + err + "]" );
                }

                return new Result( this , res , batchSize , options );
            }
            finally {
                decoder.done();
            }
        }

        public void update( DBObject query , DBObject o , boolean upsert , boolean multi )
            throws MongoException {

            if ( SHOW ) System.out.println( "update: " + _fullNameSpace + " " + JSON.serialize( query ) );
            
            OutMessage om = OutMessage.get( 2001 );
            om.writeInt( 0 ); // reserved
            om.writeCString( _fullNameSpace );
            
            int flags = 0;
            if ( upsert ) flags |= 1;
            if ( multi ) flags |= 2;
            om.writeInt( flags );

            om.putObject( query );
            om.putObject( o );
            
            _connector.say( _db , om , getWriteConcern() );
        }

        protected void createIndex( final DBObject keys, final DBObject options )
            throws MongoException {
            
            DBObject full = new BasicDBObject();
            for ( String k : options.keySet() )
                full.put( k , options.get( k ) );
            full.put( "key" , keys );

            DBApiLayer.this.doGetCollection( "system.indexes" ).insert( full , false );
        }

        final String _fullNameSpace;
    }

    static class QueryHeader {

        QueryHeader( ByteBuffer buf ){
            this( buf , buf.position() );
        }

        QueryHeader( ByteBuffer buf , int start ){
            _flags = buf.getInt( start );
            _cursor = buf.getLong( start + 4 );
            _startingFrom = buf.getInt( start + 12 );
            _num = buf.getInt( start + 16 );
        }

        int headerSize(){
            return 20;
        }

        void skipPastHeader( ByteBuffer buf ){
            buf.position( buf.position() + headerSize() );
        }

        final int _flags;
        final long _cursor;
        final int _startingFrom;
        final int _num;
    }

    class SingleResult extends QueryHeader {

        SingleResult( String fullNameSpace , ByteDecoder decoder , int options ){
            super( decoder._buf );

            _bytes = decoder.remaining();
            _fullNameSpace = fullNameSpace;
            _shortNameSpace = _removeRoot( _fullNameSpace );
            _options = options;
            skipPastHeader( decoder._buf );

            if ( _num == 0 )
                _lst = EMPTY;
            else if ( _num < 3 )
                _lst = new LinkedList<DBObject>();
            else
                _lst = new ArrayList<DBObject>( _num );

            if ( _num > 0 ){
                int num = 0;

                while( decoder.more() && num < _num ){
                    final DBObject o = decoder.readObject();

                    _lst.add( o );
                    num++;

                    if ( D ) {
                        System.out.println( "-- : " + o.keySet().size() );
                        for ( String s : o.keySet() )
                            System.out.println( "\t " + s + " : " + o.get( s ) );
                    }
                }
            }
        }

        boolean hasGetMore(){
            if ( _cursor <= 0 )
                return false;
            
            if ( _num > 0 )
                return true;

            if ( ( _options & Bytes.QUERYOPTION_TAILABLE ) == 0 )
                return false;
            
            // have a tailable cursor
            if ( ( _flags & Bytes.RESULTFLAG_AWAITCAPABLE ) > 0 )
                return true;

            try {
                System.out.println( "sleep" );
                Thread.sleep( 1000 );
            }
            catch ( Exception e ){}

            return true;
        }
        
        public String toString(){
            return "flags:" + _flags + " _cursor:" + _cursor + " _startingFrom:" + _startingFrom + " _num:" + _num ;
        }

        final long _bytes;
        final String _fullNameSpace;
        final String _shortNameSpace;
        final int _options;

        final List<DBObject> _lst;
    }

    class Result implements Iterator<DBObject> {

        Result( MyCollection coll , SingleResult res , int numToReturn , int options ){
            init( res );
            _collection = coll;
            _numToReturn = numToReturn;
            _options = options;
        }

        private void init( SingleResult res ){
            _totalBytes += res._bytes;
            _curResult = res;
            _cur = res._lst.iterator();
            _sizes.add( res._lst.size() );
        }

        public DBObject next(){
            if ( _cur.hasNext() )
                return _cur.next();

            if ( ! _curResult.hasGetMore() )
                throw new RuntimeException( "no more" );

            _advance();
            return next();
        }

        public boolean hasNext(){
            if ( _cur.hasNext() )
                return true;

            if ( ! _curResult.hasGetMore() )
                return false;

            _advance();
            return hasNext();
        }

        private void _advance(){

            if ( _curResult._cursor <= 0 )
                throw new RuntimeException( "can't advance a cursor <= 0" );
            
            OutMessage m = OutMessage.get( 2005 );

            m.writeInt( 0 ); 
            m.writeCString( _curResult._fullNameSpace );
            m.writeInt( _numToReturn ); // num to return
            m.writeLong( _curResult._cursor );
            
            ByteDecoder decoder = ByteDecoder.get( DBApiLayer.this , _collection );

            try {
                _connector.call( DBApiLayer.this , m , decoder );
                _numGetMores++;

                SingleResult res = new SingleResult( _curResult._fullNameSpace , decoder , _options );
                init( res );
            }
            catch ( MongoException me ){
                throw new MongoInternalException( "can't do getmore" , me );
            }
            finally {
                decoder.done();

            }
        }

        public void remove(){
            throw new RuntimeException( "can't remove this way" );
        }

        public String toString(){
            return "DBCursor";
        }

        protected void finalize() throws Throwable {
            if ( _curResult != null && _curResult._cursor > 0 )
                _deadCursorIds.add( _curResult._cursor );
            super.finalize();
        }

        public long totalBytes(){
            return _totalBytes;
        }
        
        int numGetMores(){
            return _numGetMores;
        }

        List<Integer> getSizes(){
            return Collections.unmodifiableList( _sizes );
        }
        
        SingleResult _curResult;
        Iterator<DBObject> _cur;
        final MyCollection _collection;
        final int _numToReturn;
        final int _options;
        
        private long _totalBytes = 0;
        private int _numGetMores = 0;
        private List<Integer> _sizes = new ArrayList<Integer>();
    }  // class Result

    final String _root;
    final String _rootPlusDot;
    final DBConnector _connector;
    final Map<String,MyCollection> _collections = Collections.synchronizedMap( new HashMap<String,MyCollection>() );
    final Map<String,DBApiLayer> _sisters = Collections.synchronizedMap( new HashMap<String,DBApiLayer>() );
    List<Long> _deadCursorIds = new Vector<Long>();

    static final List<DBObject> EMPTY = Collections.unmodifiableList( new LinkedList<DBObject>() );
}
