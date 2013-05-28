package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.MongoGroup;

public class Group extends MongoCommand {

    public Group(final MongoGroup group, final String collectionName) {
        super(asDocument(group, collectionName));
    }

    private static Document asDocument(final MongoGroup group, final String collectionName) {

        final Document document = new Document("ns", collectionName);

        if (group.getKey() != null) {
            document.put("key", group.getKey());
        } else {
            document.put("keyf", group.getKeyf());
        }

        document.put("initial", group.getInitial());
        document.put("$reduce", group.getReduce());

        if (group.getFinalize() != null) {
            document.put("finalize", group.getFinalize());
        }

        if (group.getCond() != null) {
            document.put("cond", group.getCond());
        }

        return new Document("group", document);
    }
}
