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

import org.mongodb.command.Command;
import org.mongodb.operation.Group;

import static com.mongodb.DBObjects.toNullableDocument;

/**
 * This class groups the argument for a group operation and can build the underlying command object
 *
 * @dochub mapreduce
 */
public class GroupCommand {
    private final String input;
    private final DBObject keys;
    private final DBObject condition;
    private final DBObject initial;
    private final String reduce;
    private final String finalize;

    public GroupCommand(final String input, final DBObject keys, final DBObject condition,
                        final DBObject initial, final String reduce, final String finalize) {
        this.input = input;
        this.keys = keys;
        this.condition = condition;
        this.initial = initial;
        this.reduce = reduce;
        this.finalize = finalize;
    }

    public DBObject toDBObject() {
        final DBObject args = new BasicDBObject("ns", input)
                .append("key", keys)
                .append("cond", condition)
                .append("$reduce", reduce)
                .append("initial", initial);
        if (finalize != null) {
            args.put("finalize", finalize);
        }
        return new BasicDBObject("group", args);
    }

    public Command toNew() {
        final Group group = new Group(toNullableDocument(keys), reduce, toNullableDocument(initial))
                .cond(toNullableDocument(condition))
                .finalize(finalize);

        return new org.mongodb.command.Group(group, input);
    }
}
