package com.mongodb;

import org.mongodb.command.Group;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoGroup;

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

    public MongoCommand toNew() {
        final MongoGroup mongoGroup = new MongoGroup(toNullableDocument(keys), reduce, toNullableDocument(initial))
                .cond(toNullableDocument(condition))
                .finalize(finalize);

        return new Group(mongoGroup, input);
    }
}
