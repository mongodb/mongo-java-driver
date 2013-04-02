package org.mongodb.command;

import org.mongodb.Document;

public final class ListDatabases extends MongoCommand {
    public ListDatabases() {
        super(new Document("listDatabases", 1));
    }
}
