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

/**
 * This class groups the argument for a group operation and can build the underlying command object
 *
 * @mongodb.driver.manual reference/command/group/ Group
 */
public class GroupCommand {

    /**
     * Creates a new group command.
     *
     * @param collection the collection from which to perform the group by operation.
     * @param keys       the field or fields to group.
     * @param condition  optional - a filter to determine which documents in the collection to process.
     * @param initial    the initial state of the aggregation result document.
     * @param reduce     a JavaScript aggregation function that operates on the documents during the grouping operation.
     * @param finalize   optional - a JavaScript function that runs each item in the result set before group returns the final value.
     */
    public GroupCommand(final DBCollection collection, final DBObject keys, final DBObject condition,
                        final DBObject initial, final String reduce, final String finalize) {
        this.input = collection.getName();
        this.keys = keys;
        this.condition = condition;
        this.initial = initial;
        this.reduce = reduce;
        this.finalize = finalize;
    }

    /**
     * Turns this group command into the DBObject format of the command.
     *
     * @return a DBObject containing the group command as a MongoDB document
     */
    public DBObject toDBObject() {
        BasicDBObject args  = new BasicDBObject();
        args.put( "ns" , input );
        args.put( "key" , keys );
        args.put( "cond" , condition );
        args.put( "$reduce" , reduce );
        args.put( "initial" , initial );
        if ( finalize != null )
            args.put( "finalize" , finalize );
        return new BasicDBObject( "group" , args );
    }

    String input;
    DBObject keys;
    DBObject condition;
    DBObject initial;
    String reduce;
    String finalize;
}
