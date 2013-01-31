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

import java.util.Set;

/**
 * Interface extracted from old DB class to ensure driver-compat provides the same API as the old driver code.
 */
@SuppressWarnings({ "deprecation" })
public interface IDB {
    /**
     * starts a new "consistent request". Following this call and until requestDone() is called, all db operations
     * should use the same underlying connection. This is useful to ensure that operations happen in a certain order
     * with predictable results.
     */
    void requestStart();

    /**
     * ends the current "consistent request"
     */
    void requestDone();

    /**
     * ensure that a connection is assigned to the current "consistent request" (from primary pool, if connected to a
     * replica set)
     */
    void requestEnsureConnection();

    DBCollection getCollection(String name);

    DBCollection createCollection(String name, DBObject options);

    DBCollection getCollectionFromString(String s);

    CommandResult command(DBObject cmd);

    CommandResult command(DBObject cmd, IDBCollection.DBEncoder encoder);

    CommandResult command(DBObject cmd, int options, IDBCollection.DBEncoder encoder);

    CommandResult command(DBObject cmd, int options, ReadPreference readPrefs);

    CommandResult command(DBObject cmd, int options, ReadPreference readPrefs, IDBCollection.DBEncoder encoder);

    CommandResult command(DBObject cmd, int options);

    CommandResult command(String cmd);

    CommandResult command(String cmd, int options);

    CommandResult doEval(String code, Object... args);

    Object eval(String code, Object... args);

    CommandResult getStats();

    String getName();

    void setReadOnly(Boolean b);

    Set<String> getCollectionNames();

    boolean collectionExists(String collectionName);

    @Override
    String toString();

    CommandResult getLastError();

    CommandResult getLastError(WriteConcern concern);

    CommandResult getLastError(int w, int wtimeout, boolean fsync);

    void setWriteConcern(WriteConcern concern);

    WriteConcern getWriteConcern();

    void setReadPreference(ReadPreference preference);

    ReadPreference getReadPreference();

    void dropDatabase();

    boolean isAuthenticated();

    boolean authenticate(String username, char[] password);

    CommandResult authenticateCommand(String username, char[] password);

    WriteResult addUser(String username, char[] passwd);

    WriteResult addUser(String username, char[] passwd, boolean readOnly);

    WriteResult removeUser(String username);

    CommandResult getPreviousError();

    void resetError();

    void forceError();

    Mongo getMongo();

    DB getSisterDB(String name);

    @Deprecated
    void slaveOk();

    void addOption(int option);

    void setOptions(int options);

    void resetOptions();

    int getOptions();

    void cleanCursors(boolean force);
}
