/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.annotations.ThreadSafe;
import org.mongodb.impl.MongoClientAdapter;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ThreadSafe
public class Mongo {
    private final MongoClientAdapter clientAdapter;
    private final ConcurrentMap<String, DB> dbCache = new ConcurrentHashMap<String, DB>();
    private volatile ReadPreference readPreference = ReadPreference.primary();
    private volatile WriteConcern writeConcern = WriteConcern.UNACKNOWLEDGED;

    Mongo(final List<ServerAddress> seedList, final MongoClientOptions mongoOptions) {
        this(new MongoClientAdapter(createNewSeedList(seedList), mongoOptions.toNew()));
    }

    Mongo(final MongoClientURI mongoURI) throws UnknownHostException {
        this(new MongoClientAdapter(mongoURI.toNew()));
    }

    Mongo(final ServerAddress serverAddress, final MongoClientOptions mongoOptions) {
        this(new MongoClientAdapter(serverAddress.toNew(), mongoOptions.toNew()));
    }

    Mongo(final MongoClientAdapter clientAdapter) {
        this.clientAdapter = clientAdapter;

        if (clientAdapter.getClient().getOptions().getReadPreference() != null) {
            readPreference = ReadPreference.fromNew(clientAdapter.getClient().getOptions().getReadPreference());
        }
        if (clientAdapter.getClient().getOptions().getWriteConcern() != null) {
            writeConcern = WriteConcern.fromNew(clientAdapter.getClient().getOptions().getWriteConcern());
        }
    }

    /**
     * Gets the list of server addresses currently seen by the connector. This includes addresses auto-discovered from a
     * replica set.
     *
     * @return list of server addresses
     * @throws MongoException
     */
    public List<ServerAddress> getServerAddressList() {
        List<ServerAddress> retVal = new ArrayList<ServerAddress>();
        for (org.mongodb.ServerAddress serverAddress : clientAdapter.getServerAddressList()) {
            retVal.add(new ServerAddress(serverAddress));
        }
        return retVal;
    }


    /**
     * Sets the write concern for this database. Will be used as default for writes to any collection in any database.
     * See the documentation for {@link WriteConcern} for more information.
     *
     * @param writeConcern write concern to use
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the default write concern
     *
     * @return the default write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Sets the read preference for this database. Will be used as default for reads from any collection in any
     * database. See the documentation for {@link ReadPreference} for more information.
     *
     * @param readPreference Read Preference to use
     */
    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    /**
     * Gets the default read preference
     *
     * @return the default read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }


    public DB getDB(final String dbName) {
        DB db = dbCache.get(dbName);
        if (db != null) {
            return db;
        }

        db = new DB(this, dbName);
        final DB temp = dbCache.putIfAbsent(dbName, db);
        if (temp != null) {
            return temp;
        }
        return db;
    }

    /**
     * Closes all resources associated with this instance, in particular any open network connections. Once called, this
     * instance and any databases obtained from it can no longer be used.
     */
    public void close() {
        clientAdapter.getClient().close();
    }

    org.mongodb.MongoClient getNew() {
        return clientAdapter.getClient();
    }

    //******* Missing functionality from the old driver *******/

    void requestStart() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    void requestDone() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public void addOption(final int option) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public int getOptions() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public List<String> getDatabaseNames() {
        return new ArrayList<String>(clientAdapter.getClient().tools().getDatabaseNames());
    }

    private static List<org.mongodb.ServerAddress> createNewSeedList(final List<ServerAddress> seedList) {
        List<org.mongodb.ServerAddress> retVal = new ArrayList<org.mongodb.ServerAddress>(seedList.size());
        for (ServerAddress cur : seedList) {
            retVal.add(cur.toNew());
        }
        return retVal;
    }
}
