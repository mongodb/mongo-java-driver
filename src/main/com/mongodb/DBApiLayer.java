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

import org.bson.BSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;

import static java.util.Arrays.asList;

/**
 * Concrete extension of abstract {@code DB} class.
 *
 * @deprecated This class is NOT part of the public API. It will be dropped in 3.x releases.
 */
@Deprecated
@SuppressWarnings("deprecation")
public class DBApiLayer extends DB {

    /** The maximum number of cursors allowed */
    static final int NUM_CURSORS_BEFORE_KILL = 100;
    static final int NUM_CURSORS_PER_BATCH = 20000;

    DBTCPConnector getConnector() {
        return _connector;
    }

    static int chooseBatchSize(int batchSize, int limit, int fetched) {
        int bs = Math.abs(batchSize);
        int remaining = limit > 0 ? limit - fetched : 0;
        int res;
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
     * @param connector the connector.  This must be an instance of DBTCPConnector.
     */
    protected DBApiLayer( Mongo mongo, String name , DBConnector connector ){
        super( mongo, name );

        if ( connector == null )
            throw new IllegalArgumentException( "need a connector: " + name );

        _root = name;
        _rootPlusDot = _root + ".";

        _connector = (DBTCPConnector) connector;
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

    public WriteResult addUser( String username , char[] passwd, boolean readOnly ){
        requestStart();
        try {
            if (useUserCommands(_connector.getPrimaryPort())) {
                CommandResult userInfoResult = command(new BasicDBObject("usersInfo", username));
                userInfoResult.throwOnError();
                DBObject userCommandDocument = getUserCommandDocument(username, passwd, readOnly,
                                                                      ((List) userInfoResult.get("users")).isEmpty()
                                                                      ? "createUser" : "updateUser");
                CommandResult commandResult = command(userCommandDocument);
                commandResult.throwOnError();
                return new WriteResult(commandResult, getWriteConcern());
            } else {
                return super.addUser(username, passwd, readOnly);
            }
        } finally {
            requestDone();
        }
    }

    public WriteResult removeUser( String username ){
        requestStart();
        try {
            if (useUserCommands(_connector.getPrimaryPort())) {
                CommandResult res = command(new BasicDBObject("dropUser", username));
                res.throwOnError();
                return new WriteResult(res, getWriteConcern());
            }
            else {
                return super.removeUser(username);
            }
        } finally {
            requestDone();
        }
    }

    private DBObject getUserCommandDocument(String username, char[] passwd, boolean readOnly, final String commandName) {
        return new BasicDBObject(commandName, username)
               .append("pwd", _hash(username, passwd))
               .append("digestPassword", false)
               .append("roles", Arrays.asList(getUserRoleName(readOnly)));
    }


    private String getUserRoleName(boolean readOnly) {
        return getName().equals("admin") ? (readOnly ? "readAnyDatabase" : "root") : (readOnly ? "read" : "dbOwner");
    }

    protected DBCollectionImpl doGetCollection( String name ){
        DBCollectionImpl c = _collections.get(name);
        if ( c != null )
            return c;

        c = new DBCollectionImpl(this, name );
        DBCollectionImpl old = _collections.putIfAbsent(name, c);
        return old != null ? old : c;
    }


    /**
     * @param force true if should clean regardless of number of dead cursors
     * @throws MongoException
     */
    public void cleanCursors( boolean force ){

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

    void killCursors( ServerAddress addr , List<Long> all ){
        if ( all == null || all.size() == 0 )
            return;

        OutMessage om = OutMessage.killCursors(_mongo, Math.min( NUM_CURSORS_PER_BATCH , all.size()));

        int soFar = 0;
        int totalSoFar = 0;
        for (Long l : all) {
            om.writeLong(l);

            totalSoFar++;
            soFar++;

            if ( soFar >= NUM_CURSORS_PER_BATCH ){
                _connector.say( this , om ,com.mongodb.WriteConcern.NONE );
                om = OutMessage.killCursors(_mongo, Math.min( NUM_CURSORS_PER_BATCH , all.size() - totalSoFar));
                soFar = 0;
            }
        }

        _connector.say( this , om ,com.mongodb.WriteConcern.NONE , addr );
    }

    @Override
    CommandResult doAuthenticate(MongoCredential credentials) {
        return _connector.authenticate(credentials);
    }

    private boolean useUserCommands(final DBPort port) {
        return _connector.getServerDescription(port.getAddress()).getVersion().compareTo(new ServerVersion(asList(2, 5, 4))) >= 0;
    }

    static void throwOnQueryFailure(final Response res, final long cursor) {
        if ((res._flags & Bytes.RESULTFLAG_ERRSET) > 0) {
            BSONObject errorDocument = res.get(0);
            if (ServerError.getCode(errorDocument) == 50) {
                throw new MongoExecutionTimeoutException(ServerError.getCode(errorDocument),
                                                         ServerError.getMsg(errorDocument, null));
            } else {
                throw new MongoException(ServerError.getCode(errorDocument), ServerError.getMsg(errorDocument, null));
            }
        }
        else if ((res._flags & Bytes.RESULTFLAG_CURSORNOTFOUND) > 0) {
            throw new MongoException.CursorNotFound(cursor, res.serverUsed());
        }
    }

    class Result implements Iterator<DBObject> {

        Result( DBCollectionImpl coll , Response res , int batchSize, int limit , int options, DBDecoder decoder ){
            _collection = coll;
            _batchSize = batchSize;
            _limit = limit;
            _options = options;
            _host = res._host;
            _decoder = decoder;
            init( res );
            // Only enable finalizer if cursor finalization is enabled and there is actually a cursor that needs killing
            _optionalFinalizer = coll.getDB().getMongo().getMongoOptions().isCursorFinalizerEnabled() && res.cursor() != 0 ?
                    new OptionalFinalizer() : null;
        }

        private void init( Response res ){
            throwOnQueryFailure(res, _curResult == null ? 0 : _curResult._cursor);

            _totalBytes += res._len;
            _curResult = res;
            _cur = res.iterator();
            _sizes.add( res.size() );
            _numFetched += res.size();

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
                throw new NoSuchElementException("no more");

            _advance();
            return next();
        }

        public boolean hasNext(){
            boolean hasNext = _cur.hasNext();
            while ( !hasNext ) {
                if ( ! _curResult.hasGetMore( _options ) )
                    return false;

                _advance();
                hasNext = _cur.hasNext();
                
                if (!hasNext) {
                    if ( ( _options & Bytes.QUERYOPTION_AWAITDATA ) == 0 ) {
                        // dont block waiting for data if no await
                        return false;
                    } else {
                        // if await, driver should block until data is available
                        // if server does not support await, driver must sleep to avoid busy loop
                        if ((_curResult._flags & Bytes.RESULTFLAG_AWAITCAPABLE) == 0) {
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                throw new MongoInterruptedException(e);
                            }
                        }
                    }
                }
            }
            return hasNext;
        }

        private void _advance(){

            if ( _curResult.cursor() <= 0 )
                throw new RuntimeException( "can't advance a cursor <= 0" );

            OutMessage m = OutMessage.getMore(_collection, _curResult.cursor(),
                    chooseBatchSize(_batchSize, _limit, _numFetched));

            Response res = _connector.call(_collection.getDB() , _collection , m , _host, _decoder );
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

        boolean hasFinalizer() {
            return _optionalFinalizer != null;
        }

        Response _curResult;
        Iterator<DBObject> _cur;
        int _batchSize;
        int _limit;
        final DBDecoder _decoder;
        final DBCollectionImpl _collection;
        final int _options;
        final ServerAddress _host; // host where first went.  all subsequent have to go there

        private long _totalBytes = 0;
        private int _numGetMores = 0;
        private List<Integer> _sizes = new ArrayList<Integer>();
        private int _numFetched = 0;

        // This allows us to easily enable/disable finalizer for cleaning up un-closed cursors
        private final OptionalFinalizer _optionalFinalizer;

        private class OptionalFinalizer {
            @Override
            protected void finalize() {
                if (_curResult != null) {
                    long curId = _curResult.cursor();
                    _curResult = null;
                    _cur = null;
                    if (curId != 0) {
                        _deadCursorIds.add(new DeadCursor(curId, _host));
                    }
                }
            }
        }

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
    final DBTCPConnector _connector;
    final ConcurrentHashMap<String,DBCollectionImpl> _collections = new ConcurrentHashMap<String,DBCollectionImpl>();

    ConcurrentLinkedQueue<DeadCursor> _deadCursorIds = new ConcurrentLinkedQueue<DeadCursor>();

}
