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

// MapReduceOutput.java

package com.mongodb;

/**
 * Represents the result of a map/reduce operation
 *
 * @author antoine
 */
public class MapReduceOutput {

    private final CommandResult commandResult;
    private final Iterable<DBObject> results;
    private final DBCollection collection;
    private final DBObject command;

    @SuppressWarnings("unchecked")
    public MapReduceOutput(final DBCollection inputCollection, final DBObject command, final CommandResult commandResult) {
        this.commandResult = commandResult;
        this.command = command;
        if (commandResult.containsField("results")) {
            // means we called command with OutputType.INLINE
            collection = null;
            results = (Iterable<DBObject>) commandResult.get("results");
        } else {
            this.collection = determineOutputCollection(inputCollection, commandResult);
            this.collection.setReadPreference(ReadPreference.primary());
            this.results = collection.find();
        }
    }

    /**
     * returns a cursor to the results of the operation
     *
     * @return
     */
    public Iterable<DBObject> results() {
        return results;
    }

    /**
     * drops the collection that holds the results
     *
     * @throws MongoException
     */
    public void drop() {
        if (collection != null) {
            collection.drop();
        }
    }

    /**
     * Gets the collection that holds the results (Will return null if results are Inline)
     *
     * @return
     */
    public DBCollection getOutputCollection() {
        return collection;
    }

    @Deprecated
    public BasicDBObject getRaw() {
        return commandResult;
    }

    public CommandResult getCommandResult() {
        return commandResult;
    }

    public DBObject getCommand() {
        return command;
    }

    public ServerAddress getServerUsed() {
        return commandResult.getServerUsed();
    }

    public String toString() {
        return commandResult.toString();
    }

    private DBCollection determineOutputCollection(final DBCollection inputCollection, final CommandResult commandResult) {
        final Object result = commandResult.get("result");
        if (result instanceof String) {
            // we have only infomation about collection name,
            // that means that outputCollection in the same database with inputCollection
            final String collectionName = (String) result;
            return inputCollection.getDB().getCollection(collectionName);
        } else if (result instanceof BasicDBObject) {
            // we have complex object,
            // if specified we will use another database for output collection
            final BasicDBObject output = (BasicDBObject) result;
            final DB db = output.containsField("db")
                    ? inputCollection.getDB().getSisterDB(output.getString("db"))
                    : inputCollection.getDB();
            return db.getCollection(output.getString("collection"));
        } else {
            throw new IllegalStateException("Unexpected output target for Map-Reduce command");
        }
    }
}
