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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.BSONObject;
import org.bson.types.ObjectId;

import com.mongodb.util.JSON;

/** Database API
 * This cannot be directly instantiated, but the functions are available
 * through instances of Mongo.
 */
public class DBApiLayer extends DB {

    static final boolean D = Boolean.getBoolean( "DEBUG.DB" );
    /** The maximum number of cursors allowed */
    static final int NUM_CURSORS_BEFORE_KILL = 100;
    static final int NUM_CURSORS_PER_BATCH = 20000;
    
    //  --- show

    static final Logger TRACE_LOGGER = Logger.getLogger( "com.mongodb.TRACE" );
    static final Level TRACE_LEVEL = Boolean.getBoolean( "DB.TRACE" ) ? Level.INFO : Level.FINEST;

    static final boolean willTrace(){
        return TRACE_LOGGER.isLoggable( TRACE_LEVEL );
    }
    
    static final void trace( String s ){
        TRACE_LOGGER.log( TRACE_LEVEL , s );
    }

    static int chooseBatchSize(int batchSize, int limit, int fetched) {
        int bs = Math.abs(batchSize);
        int remaining = limit > 0 ? limit - fetched : 0;
        int res = 0;
        if (bs == 0 && remaining > 0)
            res = remaining;
        else if (bs > 0 && remaining == 0)
            res = bs;
        else
            res = Math.min(bs, remaining);

        if (batchSize < 0) {
            // force close
            res = -res;
        }

        if (res == 1) {
            // optimization: use negative batchsize to close cursor
            res = -1;
        }
        return res;
    }

    /**
     * @param mongo the Mongo instance
     * @param name the database name
     * @param connector the connector
     */
    protected DBApiLayer( Mongo mongo, String name , DBConnector connector ){
        super( mongo, name );

        if ( connector == null )
            throw new IllegalArgumentException( "need a connector: " + name );
        
        _root = name;
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

    public void cleanCursors( boolean force )
        throws MongoException {

        int sz = _deadCursorIds.size();

        if ( sz == 0 || ( ! force && sz < NUM_CURSORS_BEFORE_KILL))
            return;

        Bytes.LOGGER.info( "going to kill cursors : " + sz );

        Map<ServerAddress,List<Long>> m = new HashMap<ServerAddress,List<Long>>();
        DeadCursor c;
        while (( c = _deadCursorIds.poll()) != null ){
            List<Long> x = m.get( c.host );
            if ( x == null ){
                x = new LinkedList<Long>();
                m.put( c.host , x );
            }
            x.add( c.id );
        }
            
        for ( Map.Entry<ServerAddress,List<Long>> e : m.entrySet() ){
            try {
                killCursors( e.getKey() , e.getValue() );
            }
            catch ( Throwable t ){
                Bytes.LOGGER.log( Level.WARNING , "can't clean cursors" , t );
                for ( Long x : e.getValue() )
                        _deadCursorIds.add( new DeadCursor( x , e.getKey() ) );
            }
        }
    }

    void killCursors( ServerAddress addr , List<Long> all )
        throws MongoException {
        if ( all == null || all.size() == 0 )
            return;

        OutMessage om = new OutMessage( _mongo , 2007 );
        om.writeInt( 0 ); // reserved
            
        om.writeInt( Math.min( NUM_CURSORS_PER_BATCH , all.size() ) );

        int soFar = 0;
        int totalSoFar = 0;
        for (Long l : all) {
            om.writeLong(l);

            totalSoFar++;
            soFar++;

            if ( soFar >= NUM_CURSORS_PER_BATCH ){
                _connector.say( this , om ,com.mongodb.WriteConcern.NONE );
                om = new OutMessage( _mongo , 2007 );
                om.writeInt( 0 ); // reserved
                om.writeInt( Math.min( NUM_CURSORS_PER_BATCH , all.size() - totalSoFar ) );
                soFar = 0;
            }
        }

        _connector.say( this , om ,com.mongodb.WriteConcern.NONE , addr );
    }

    class MyCollection extends DBCollection {
        MyCollection( String name ){
            super( DBApiLayer.this , name );
            _fullNameSpace = _root + "." + name;
        }

        public void doapply( DBObject o ){
        }

        @Override
        public void drop() throws MongoException {
            _collections.remove(getName());
            super.drop();
        }

        public WriteResult insert(DBObject[] arr, com.mongodb.WriteConcern concern )
            throws MongoException {
            return insert( arr , true , concern );
        }

        protected WriteResult insert(DBObject[] arr, boolean shouldApply , com.mongodb.WriteConcern concern )
            throws MongoException {

            if ( willTrace() ) {
                for (DBObject o : arr) {
                    trace( "save:  " + _fullNameSpace + " " + JSON.serialize( o ) );
                }
            }
            
            if ( shouldApply ){
                for ( int i=0; i<arr.length; i++ ){
                    DBObject o=arr[i];
                    apply( o );
                    _checkObject( o , false , false );
                    Object id = o.get( "_id" );
                    if ( id instanceof ObjectId ){
                        ((ObjectId)id).notNew();
                    }
                }
            }

            WriteResult last = null;

            int cur = 0;
            int maxsize = _mongo.getMaxBsonObjectSize();
            while ( cur < arr.length ){
                OutMessage om = new OutMessage( _mongo , 2002 );
                
                om.writeInt( 0 ); // reserved
                om.writeCString( _fullNameSpace );
                
                for ( ; cur<arr.length; cur++ ){
                    DBObject o = arr[cur];
                    om.putObject( o );

                    // limit for batch insert is 4 x maxbson on server, use 2 x to be safe
                    if ( om.size() > 2 * maxsize ){
                        cur++;
                        break;
                    }
                }
                
                last = _connector.say( _db , om , concern );
            }
            
            return last;
        }
        
        public WriteResult remove( DBObject o , com.mongodb.WriteConcern concern )
            throws MongoException {

            if ( willTrace() ) trace( "remove: " + _fullNameSpace + " " + JSON.serialize( o ) );

            OutMessage om = new OutMessage( _mongo , 2006 );

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
            
            return _connector.say( _db , om , concern );
        }

        @Override
        Iterator<DBObject> __find( DBObject ref , DBObject fields , int numToSkip , int batchSize, int limit , int options )
            throws MongoException {
            
            if ( ref == null )
                ref = new BasicDBObject();
            
            if ( willTrace() ) trace( "find: " + _fullNameSpace + " " + JSON.serialize( ref ) );
            
            OutMessage query = OutMessage.query( _mongo , options , _fullNameSpace , numToSkip , chooseBatchSize(batchSize, limit, 0) , ref , fields );

            Response res = _connector.call( _db , this , query , null , 2 );

            if ( res.size() == 0 )
                return null;
            
            if ( res.size() == 1 ){
                BSONObject foo = res.get(0);
                MongoException e = MongoException.parse( foo );
                if ( e != null && ! _name.equals( "$cmd" ) )
                    throw e;
            }
            
            return new Result( this , res , batchSize, limit , options );
        }

        @Override
        public WriteResult update( DBObject query , DBObject o , boolean upsert , boolean multi , com.mongodb.WriteConcern concern )
            throws MongoException {

            if (o != null && !o.keySet().isEmpty()) {
                // if 1st key doesn't start with $, then object will be inserted as is, need to check it
                String key = o.keySet().iterator().next();
                if (key.charAt(0) != '$')
                    _checkObject(o, false, false);
            }

            if ( willTrace() ) trace( "update: " + _fullNameSpace + " " + JSON.serialize( query ) + " " + JSON.serialize( o )  );
            
            OutMessage om = new OutMessage( _mongo , 2001 );
            om.writeInt( 0 ); // reserved
            om.writeCString( _fullNameSpace );
            
            int flags = 0;
            if ( upsert ) flags |= 1;
            if ( multi ) flags |= 2;
            om.writeInt( flags );

            om.putObject( query );
            om.putObject( o );
            
            return _connector.say( _db , om , concern );
        }
        
        public void createIndex( final DBObject keys, final DBObject options )
            throws MongoException {
            
            DBObject full = new BasicDBObject();
            for ( String k : options.keySet() )
                full.put( k , options.get( k ) );
            full.put( "key" , keys );

            DBApiLayer.this.doGetCollection( "system.indexes" ).insert( new DBObject[]{ full } , false , WriteConcern.SAFE );
        }

        final String _fullNameSpace;
    }

    class Result implements Iterator<DBObject> {

        Result( MyCollection coll , Response res , int batchSize, int limit , int options ){
            _collection = coll;
            _batchSize = batchSize;
            _limit = limit;
            _options = options;
            _host = res._host;
            init( res );
        }

        private void init( Response res ){
            _totalBytes += res._len;
            _curResult = res;
            _cur = res.iterator();
            _sizes.add( res.size() );
            _numFetched += res.size();

            if ( ( res._flags & Bytes.RESULTFLAG_CURSORNOTFOUND ) > 0 ){
                throw new MongoException.CursorNotFound();
            }

            if (res._cursor != 0 && _limit > 0 && _limit - _numFetched <= 0) {
                // fetched all docs within limit, close cursor server-side
                killCursor();
            }
        }

        public DBObject next(){
            if ( _cur.hasNext() ) {
                return _cur.next();
            }

            if ( ! _curResult.hasGetMore( _options ) )
                throw new RuntimeException( "no more" );

            _advance();
            return next();
        }

        public boolean hasNext(){
            while ( true ){
                if ( _cur.hasNext() )
                    return true;
                
                if ( ! _curResult.hasGetMore( _options ) )
                    return false;
                
                _advance();
            }
        }

        private void _advance(){

            if ( _curResult.cursor() <= 0 )
                throw new RuntimeException( "can't advance a cursor <= 0" );
            
            OutMessage m = new OutMessage( _mongo , 2005 );

            m.writeInt( 0 ); 
            m.writeCString( _collection._fullNameSpace );
            m.writeInt( chooseBatchSize(_batchSize, _limit, _numFetched) );
            m.writeLong( _curResult.cursor() );
            
            Response res = _connector.call( DBApiLayer.this , _collection , m , _host );
            _numGetMores++;
            init( res );
        }

        public void remove(){
            throw new RuntimeException( "can't remove this way" );
        }
        
        public int getBatchSize(){
            return _batchSize;
        }
        
        public void setBatchSize(int size){
            _batchSize = size;
        }

        public String toString(){
            return "DBCursor";
        }

        protected void finalize() throws Throwable {
            if (_curResult != null) {
                long curId = _curResult.cursor();
                _curResult = null;
                _cur = null;
                if (curId != 0) {
                    _deadCursorIds.add(new DeadCursor(curId, _host));
                }
            }
            super.finalize();
        }

        public long totalBytes(){
            return _totalBytes;
        }
        
        public long getCursorId(){
            if ( _curResult == null )
                return 0;
            return _curResult._cursor;
        }
        
        int numGetMores(){
            return _numGetMores;
        }

        List<Integer> getSizes(){
            return Collections.unmodifiableList( _sizes );
        }
        
        void close(){
            // not perfectly thread safe here, may need to use an atomicBoolean
            if (_curResult != null) {
                killCursor();
                _curResult = null;
                _cur = null;
            }
        }

        void killCursor() {
            if (_curResult == null)
                return;
            long curId = _curResult.cursor();
            if (curId == 0)
                return;

            List<Long> l = new ArrayList<Long>();
            l.add(curId);

            try {
                killCursors(_host, l);
            } catch (Throwable t) {
                Bytes.LOGGER.log(Level.WARNING, "can't clean 1 cursor", t);
                _deadCursorIds.add(new DeadCursor(curId, _host));
            }
            _curResult._cursor = 0;
        }

        public ServerAddress getServerAddress() {
            return _host;
        }
        
        Response _curResult;
        Iterator<DBObject> _cur;
        int _batchSize;
        int _limit;
        final MyCollection _collection;
        final int _options;
        final ServerAddress _host; // host where first went.  all subsequent have to go there

        private long _totalBytes = 0;
        private int _numGetMores = 0;
        private List<Integer> _sizes = new ArrayList<Integer>();
        private int _numFetched = 0;
        
    }  // class Result
    
    static class DeadCursor {
        
        DeadCursor( long a , ServerAddress b ){
            id = a;
            host = b;
        }

        final long id;
        final ServerAddress host;
    }
    
    final String _root;
    final String _rootPlusDot;
    final DBConnector _connector;
    final Map<String,MyCollection> _collections = Collections.synchronizedMap( new HashMap<String,MyCollection>() );
    
    ConcurrentLinkedQueue<DeadCursor> _deadCursorIds = new ConcurrentLinkedQueue<DeadCursor>();

    static final List<DBObject> EMPTY = Collections.unmodifiableList( new LinkedList<DBObject>() );
}
