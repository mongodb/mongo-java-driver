/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import com.mongodb.serializers.CollectibleDBObjectSerializer;
import org.mongodb.MongoDatabase;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.ObjectIdGenerator;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DB {
    private final Mongo mongo;
    private final MongoDatabase database;
    private final ConcurrentHashMap<String, DBCollection> collectionCache = new ConcurrentHashMap<String, DBCollection>();
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    DB(final Mongo mongo, final String dbName) {
        this.mongo = mongo;
        database = mongo.getNew().getDatabase(dbName);
    }

    public ReadPreference getReadPreference() {
        return readPreference != null ? readPreference : mongo.getReadPreference();
    }

    public WriteConcern getWriteConcern() {
        return writeConcern != null ? writeConcern : mongo.getWriteConcern();
    }

    /**
     * starts a new "consistent request". Following this call and until requestDone() is called, all db operations
     * should use the same underlying connection. This is useful to ensure that operations happen in a certain order
     * with predictable results.
     */
    public void requestStart() {
        mongo.requestStart();
    }

    /**
     * ends the current "consistent request"
     */
    public void requestDone() {
        mongo.requestDone();
    }

    /**
     * ensure that a connection is assigned to the current "consistent request" (from primary pool, if connected to a
     * replica set)
     */
    public void requestEnsureConnection() {
        requestStart();
    }


    public DBCollection getCollection(final String name) {
        DBCollection collection = collectionCache.get(name);
        if (collection != null) {
            return collection;
        }

        final PrimitiveSerializers primitiveSerializers = PrimitiveSerializers.createDefault();
        collection = new DBCollection(database.getTypedCollection(name,
                primitiveSerializers,
                new CollectibleDBObjectSerializer(this, primitiveSerializers, new ObjectIdGenerator())), this);
        final DBCollection old = collectionCache.putIfAbsent(name, collection);
        return old != null ? old : collection;
    }

    /**
     * Drops this database. Removes all data on disk. Use with caution.
     * @throws MongoException
     */
    public void dropDatabase(){
        database.admin().drop();
    }
    /**
     * Returns a collection matching a given string.
     * @param s the name of the collection
     * @return the collection
     */
    public DBCollection getCollectionFromString( String s ){
        DBCollection foo = null;

        int idx = s.indexOf( "." );
        while ( idx >= 0 ){
            String b = s.substring( 0 , idx );
            s = s.substring( idx + 1 );
            if ( foo == null )
                foo = getCollection( b );
            else
                foo = foo.getCollection( b );
            idx = s.indexOf( "." );
        }

        if ( foo != null )
            return foo.getCollection( s );
        return getCollection( s );
    }

    public String getName() {
        return database.getName();
    }

    /**
     * Returns a set containing the names of all collections in this database.
     * @return the names of collections in this database
     * @throws MongoException
     */
    public Set<String> getCollectionNames() {
        return database.admin().getCollectionNames();
    }

    public void createCollection(final String collName, final DBObject options) {
        boolean capped = false;
        int sizeInBytes = 0;
        boolean autoIndex = true;
        if (options.get("capped") != null) {
            capped = (Boolean) options.get("capped");
        }
        if (options.get("size") != null) {
            sizeInBytes = (Integer) options.get("size");
        }
        if (options.get("autoIndexId") != null) {
            autoIndex = (Boolean) options.get("autoIndexId");
        }
        database.admin().createCollection(collName, capped, sizeInBytes, autoIndex);
    }

    public boolean authenticate(final String username, final char[] password) {
        throw new UnsupportedOperationException();
    }

    public CommandResult command(final DBObject cmd) {
        org.mongodb.result.CommandResult baseCommandResult = database.executeCommand(
                new MongoCommandOperation(DBObjects.toCommandDocument(cmd)).readPreference(getReadPreference().toNew()));
        return DBObjects.toCommandResult(cmd, new ServerAddress(baseCommandResult.getAddress()), baseCommandResult.getResponse());
    }

    /**
     * Executes a database command.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd dbobject representing the command to execute
     * @param options query options to use
     * @param readPrefs ReadPreferences for this command (nodes selection is the biggest part of this)
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options, ReadPreference readPrefs ){
//        readPrefs = getCommandReadPreference(cmd, readPrefs);
//        cmd = wrapCommand(cmd, readPrefs);
//
//        Iterator<DBObject> i =
//                getCollection("$cmd").__find(cmd, new BasicDBObject(), 0, -1, 0, options, readPrefs ,
//                                             DefaultDBDecoder.FACTORY.create(), encoder);
//        if ( i == null || ! i.hasNext() )
//            return null;
//
//        DBObject res = i.next();
//        ServerAddress sa = (i instanceof Result) ? ((Result) i).getServerAddress() : null;
//        CommandResult cr = new CommandResult(cmd, sa);
//        cr.putAll( res );
//        return cr;
        throw new UnsupportedOperationException();
    }

    /**
     * Gets another database on same server
     * @param name name of the database
     * @return
     */
    public DB getSisterDB( String name ){
        return mongo.getDB( name );
    }

    MongoDatabase toNew() {
        return database;
    }
}
