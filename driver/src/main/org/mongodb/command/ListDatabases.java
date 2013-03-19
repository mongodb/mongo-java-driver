package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.MongoCommand;

public final class ListDatabases extends MongoCommand {
    public ListDatabases() {
        super(new Document("listDatabases", 1));
    }
}
