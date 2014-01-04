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
            // Only enable finalizer if cursor finalization is enabled and there is actually a cursor that needs killing
            _optionalFinalizer = coll.getDB().getMongo().getMongoOptions().isCursorFinalizerEnabled() && res.cursor() != 0 ?
                    new OptionalFinalizer() : null;
            init( res );
        }

        public DBObject next() {
            while (true) {
                if (!(hasNext() && !_cur.hasNext())) {
                    break;
                }
            }

            if (!hasNext()) {
                throw new NoSuchElementException("no more documents in query result iterator");
            }

            return _cur.next();
        }

        public boolean hasNext(){
            if (_cur.hasNext()) {
                return true;
            }

            if (!hasGetMore()) {
                return false;
            }

            _advance();

            return _cur.hasNext() || awaitResultsFromTailable();
        }

        private void _advance(){
            if (_cursorId == 0) {
                throw new MongoInternalException("can't advance when there is no cursor id");
            }

            OutMessage m = OutMessage.getMore(_collection, _cursorId, chooseBatchSize(_batchSize, _limit, _numFetched));

            Response res = _connector.call(_collection.getDB() , _collection , m , _host, _decoder );
            _numGetMores++;
            init( res );
        }

        public void remove(){
            throw new UnsupportedOperationException("can't remove a document via a query result iterator");
        }

        public void setBatchSize(int size){
            _batchSize = size;
        }

        public long getCursorId(){
            return _cursorId;
        }

        int numGetMores(){
            return _numGetMores;
        }

        List<Integer> getSizes(){
            return Collections.unmodifiableList( _sizes );
        }

        void close(){
            if (!closed) {
                closed = true;
                killCursor();
            }
        }

        private void init( Response res ){
            throwOnQueryFailure(res, _cursorId);

            _cursorId = res.cursor();
            _cur = res.iterator();
            _curSize = res.size();
            _sizes.add( res.size() );
            _numFetched += res.size();

            if (res._cursor != 0 && _limit > 0 && _limit - _numFetched <= 0) {
                // fetched all docs within limit, close cursor server-side
                killCursor();
            }
        }

        public boolean hasGetMore() {
            if (_cursorId == 0) {
                return false;
            }

            if (_curSize > 0) {
                return true;
            }

            if (!isTailable()) {
                return false;
            }

            // have a tailable cursor, it is always possible to call get more
            return true;
        }

        private boolean isTailable() {
            return (_options & Bytes.QUERYOPTION_TAILABLE) != 0;
        }

        private boolean awaitResultsFromTailable() {
            return isTailable() && (_options & Bytes.QUERYOPTION_AWAITDATA) != 0;
        }

        void killCursor() {
            if (_cursorId == 0)
                return;

            try {
                killCursors(_host, asList(_cursorId));
                _cursorId = 0;
            } catch (MongoException e) {
                addDeadCursor(new DeadCursor(_cursorId, _host));
            }
        }

        public ServerAddress getServerAddress() {
            return _host;
        }

        boolean hasFinalizer() {
            return _optionalFinalizer != null;
        }

        private final DBDecoder _decoder;
        private final DBCollectionImpl _collection;
        private final int _options;
        private final ServerAddress _host;
        private final int _limit;

        private long _cursorId;
        private Iterator<DBObject> _cur;
        private int _curSize;
        private int _batchSize;

        private boolean closed;

        private final List<Integer> _sizes = new ArrayList<Integer>();
        private int _numGetMores = 0;
        private int _numFetched = 0;

        // This allows us to easily enable/disable finalizer for cleaning up un-closed cursors
        private final OptionalFinalizer _optionalFinalizer;

        private class OptionalFinalizer {
            @Override
            protected void finalize() throws Throwable {
                if (!closed && _cursorId != 0) {
                    addDeadCursor(new DeadCursor(_cursorId, _host));
                }
                super.finalize();
            }
        }

    }

    void addDeadCursor(final DeadCursor deadCursor) {
        _deadCursorIds.add(deadCursor);
    }

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
