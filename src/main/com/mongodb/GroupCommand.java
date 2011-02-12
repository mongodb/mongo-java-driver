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

/**
 * This class groups the argument for a group operation and can build the underlying command object
 * @dochub mapreduce
 */
public class GroupCommand {

    public GroupCommand(DBCollection inputCollection, DBObject keys, DBObject condition, DBObject initial, String reduce, String finalize) {
        this.input = inputCollection.getName();
        this.keys = keys;
        this.condition = condition;
        this.initial = initial;
        this.reduce = reduce;
        this.finalize = finalize;
    }

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
