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

import com.mongodb.serializers.DocumentSerializer;
import org.mongodb.CreateCollectionOptions;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.operation.MongoCommand;
import org.mongodb.serialization.PrimitiveSerializers;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class DB implements IDB {
    private final Mongo mongo;
    private final MongoDatabase database;
    private final ConcurrentHashMap<String, DBCollection> collectionCache = new ConcurrentHashMap<String,
            DBCollection>();
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    DB(final Mongo mongo, final String dbName) {
        this.mongo = mongo;
        database = mongo.getNew().getDatabase(dbName, MongoDatabaseOptions.builder()
                .primitiveSerializers(PrimitiveSerializers.createDefault())
                .documentSerializer(new DocumentSerializer(PrimitiveSerializers.createDefault())).build());
    }

    /**
     * Gets the Mongo instance
     *
     * @return the mongo instance that this database was created from.
     */
    public Mongo getMongo() {
        return mongo;
    }

    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    public ReadPreference getReadPreference() {
        return readPreference != null ? readPreference : mongo.getReadPreference();
    }

    public WriteConcern getWriteConcern() {
        return writeConcern != null ? writeConcern : mongo.getWriteConcern();
    }

    /**
     * Starts a new "consistent request". Following this call and until requestDone() is called, all db operations
     * should use the same underlying connection. This is useful to ensure that operations happen in a certain order
     * with predictable results.
     */
    public void requestStart() {
        mongo.requestStart();
    }

    /**
     * Ends the current "consistent request"
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

        collection = new DBCollection(name, this);
        final DBCollection old = collectionCache.putIfAbsent(name, collection);
        return old != null ? old : collection;
    }

    /**
     * Drops this database. Removes all data on disk. Use with caution.
     *
     * @throws MongoException
     */
    public void dropDatabase() {
        database.admin().drop();
    }

    /**
     * Returns a collection matching a given string.
     *
     * @param s the name of the collection
     * @return the collection
     */
    public DBCollection getCollectionFromString(String s) {
        DBCollection foo = null;

        int idx = s.indexOf(".");
        while (idx >= 0) {
            final String b = s.substring(0, idx);
            s = s.substring(idx + 1);
            if (foo == null) {
                foo = getCollection(b);
            }
            else {
                foo = foo.getCollection(b);
            }
            idx = s.indexOf(".");
        }

        if (foo != null) {
            return foo.getCollection(s);
        }
        return getCollection(s);
    }

    public String getName() {
        return database.getName();
    }

    /**
     * Returns a set containing the names of all collections in this database.
     *
     * @return the names of collections in this database
     * @throws MongoException
     */
    public Set<String> getCollectionNames() {
        return database.admin().getCollectionNames();
    }

    public DBCollection createCollection(final String collName, final DBObject options) {
        boolean capped = false;
        int sizeInBytes = 0;
        boolean autoIndex = true;
        int maxDocuments = 0;
        if (options.get("capped") != null) {
            capped = (Boolean) options.get("capped");
        }
        if (options.get("size") != null) {
            sizeInBytes = ((Number) options.get("size")).intValue();
        }
        if (options.get("autoIndexId") != null) {
            autoIndex = (Boolean) options.get("autoIndexId");
        }
        if (options.get("max") != null) {
            maxDocuments = ((Number) options.get("max")).intValue();
        }
        database.admin().createCollection(
                new CreateCollectionOptions(collName, capped, sizeInBytes, autoIndex, maxDocuments));
        // TODO the old code returned a DBCollection
        return null;
    }

    public boolean authenticate(final String username, final char[] password) {
        return false;  // TODO: Implement authentication!!!!
    }

    /**
     * Executes a database command. This method constructs a simple DBObject using cmd as the field name and {@code
     * true} as its valu, and calls {@link DB#command(com.mongodb.DBObject) }
     *
     * @param cmd command to execute
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command(final String cmd) {
        return command(new BasicDBObject(cmd, Boolean.TRUE));
    }

    /**
     * Executes a database command.
     *
     * @param cmd document representing the command to execute
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command(final DBObject cmd) {
        final org.mongodb.result.CommandResult baseCommandResult = database.executeCommand(
                new MongoCommand(DBObjects.toCommandDocument(cmd)).readPreference(
                        getReadPreference().toNew()));
        return DBObjects.toCommandResult(cmd, new ServerAddress(baseCommandResult.getAddress()),
                                         baseCommandResult.getResponse());
    }

    /**
     * Executes a database command.
     *
     * @param cmd       dbobject representing the command to execute
     * @param options   query options to use
     * @param readPrefs ReadPreferences for this command (nodes selection is the biggest part of this)
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     */
    public CommandResult command(final DBObject cmd, final int options, final ReadPreference readPrefs) {
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
     *
     * @param name name of the database
     * @return
     */
    public DB getSisterDB(final String name) {
        return mongo.getDB(name);
    }

    MongoDatabase toNew() {
        return database;
    }

    @Override
    public CommandResult command(final DBObject cmd, final IDBCollection.DBEncoder encoder) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult command(final DBObject cmd, final int options, final IDBCollection.DBEncoder encoder) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult command(final DBObject cmd, final int options, final ReadPreference readPrefs, final
    IDBCollection.DBEncoder encoder) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult command(final DBObject cmd, final int options) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult command(final String cmd, final int options) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult doEval(final String code, final Object... args) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public Object eval(final String code, final Object... args) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult getStats() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void setReadOnly(final Boolean b) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public boolean collectionExists(final String collectionName) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult getLastError() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult getLastError(final WriteConcern concern) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult getLastError(final int w, final int wtimeout, final boolean fsync) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public boolean isAuthenticated() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult authenticateCommand(final String username, final char[] password) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public WriteResult addUser(final String username, final char[] passwd) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public WriteResult addUser(final String username, final char[] passwd, final boolean readOnly) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public WriteResult removeUser(final String username) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult getPreviousError() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void resetError() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void forceError() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void slaveOk() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void addOption(final int option) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void setOptions(final int options) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void resetOptions() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public int getOptions() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void cleanCursors(final boolean force) {
        throw new IllegalStateException("Not implemented yet!");
    }
}
