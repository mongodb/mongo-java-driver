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

import java.util.*;
import java.util.logging.*;

import org.bson.*;
import org.bson.types.*;

import com.mongodb.util.*;

/** Database API
 * This cannot be directly instantiated, but the functions are available
 * through instances of Mongo.
 */
public class DBApiLayer extends DB {

    static final boolean D = Boolean.getBoolean( "DEBUG.DB" );
    /** The maximum number of cursors allowed */
    static final int NUM_CURSORS_BEFORE_KILL = 100;

    static final boolean SHOW = Boolean.getBoolean( "DB.SHOW" );

    protected DBApiLayer( Mongo mongo , String root , DBConnector connector ){
        super( mongo , root );

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


    class MyCollection extends DBCollection {
        MyCollection( String name ){
            super( DBApiLayer.this , name );
            _fullNameSpace = _root + "." + name;
        }

        public void doapply( DBObject o ){
        }

        public WriteResult insert(DBObject ... arr)
            throws MongoException {
            return insert(arr, true);
        }

        public WriteResult insert(List<DBObject> list)
            throws MongoException {
            return insert(list.toArray(new DBObject[list.size()]) , true);
        }

        protected WriteResult insert(DBObject obj, boolean shouldApply )
            throws MongoException {
            return insert( new DBObject[]{ obj } , shouldApply );
        }

        protected WriteResult insert(DBObject[] arr, boolean shouldApply )
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
            
            WriteResult last = null;

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
                
                last = _connector.say( _db , om , getWriteConcern() );
            }
            
            return last;
        }
        
        public WriteResult remove( DBObject o )
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
            
            return _connector.say( _db , om , getWriteConcern() );
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

        @Override
        Iterator<DBObject> __find( DBObject ref , DBObject fields , int numToSkip , int batchSize , int options )
            throws MongoException {
            
            if ( ref == null )
                ref = new BasicDBObject();
            
            if ( SHOW ) System.out.println( "find: " + _fullNameSpace + " " + JSON.serialize( ref ) );

            _cleanCursors();
            
            OutMessage query = OutMessage.query( options , _fullNameSpace , numToSkip , batchSize , ref , fields );

            Response res = _connector.call( _db , this , query , 2 );

            if ( res.size() == 0 )
                return null;
            
            if ( res.size() == 1 ){
                BSONObject foo = res.get(0);
                MongoException e = MongoException.parse( foo );
                if ( e != null )
                    throw e;
            }
            
            return new Result( this , res , batchSize , options );
        }

        public WriteResult update( DBObject query , DBObject o , boolean upsert , boolean multi )
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
            
            return _connector.say( _db , om , getWriteConcern() );
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

    class Result implements Iterator<DBObject> {

        Result( MyCollection coll , Response res , int numToReturn , int options ){
            init( res );
            _collection = coll;
            _numToReturn = numToReturn;
            _options = options;
        }

        private void init( Response res ){
            _totalBytes += res._len;
            _curResult = res;
            _cur = res.iterator();
            _sizes.add( res.size() );

            if ( ( res._flags & Bytes.RESULTFLAG_CURSORNOTFOUND ) > 0 ){
                throw new MongoException.CursorNotFound();
            }
        }

        public DBObject next(){
            if ( _cur.hasNext() )
                return _cur.next();

            if ( ! _curResult.hasGetMore( _options ) )
                throw new RuntimeException( "no more" );

            _advance();
            return next();
        }

        public boolean hasNext(){
            if ( _cur.hasNext() )
                return true;

            if ( ! _curResult.hasGetMore( _options ) )
                return false;

            _advance();
            return hasNext();
        }

        private void _advance(){

            if ( _curResult.cursor() <= 0 )
                throw new RuntimeException( "can't advance a cursor <= 0" );
            
            OutMessage m = OutMessage.get( 2005 );

            m.writeInt( 0 ); 
            m.writeCString( _collection._fullNameSpace );
            m.writeInt( _numToReturn ); // num to return
            m.writeLong( _curResult.cursor() );
            
            try {
                Response res = _connector.call( DBApiLayer.this , _collection , m );
                _numGetMores++;
                init( res );
            }
            catch ( MongoException me ){
                throw new MongoInternalException( "can't do getmore" , me );
            }
        }

        public void remove(){
            throw new RuntimeException( "can't remove this way" );
        }
        
        public int getNumberToReturn(){
        	return _numToReturn;
        }

        public void setNumberToReturn(int num){
        	_numToReturn = num;
        }

        public String toString(){
            return "DBCursor";
        }

        protected void finalize() throws Throwable {
            if ( _curResult != null && _curResult.cursor() > 0 )
                _deadCursorIds.add( _curResult.cursor() );
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
        
        
        
        Response _curResult;
        Iterator<DBObject> _cur;
        int _numToReturn;
        final MyCollection _collection;
        final int _options;
        
        private long _totalBytes = 0;
        private int _numGetMores = 0;
        private List<Integer> _sizes = new ArrayList<Integer>();
    }  // class Result

    final String _root;
    final String _rootPlusDot;
    final DBConnector _connector;
    final Map<String,MyCollection> _collections = Collections.synchronizedMap( new HashMap<String,MyCollection>() );
    List<Long> _deadCursorIds = new Vector<Long>();

    static final List<DBObject> EMPTY = Collections.unmodifiableList( new LinkedList<DBObject>() );
}
