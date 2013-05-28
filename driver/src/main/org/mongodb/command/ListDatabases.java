package org.mongodb.command;

import org.mongodb.Document;

public final class ListDatabases extends Command {
    public ListDatabases() {
        super(new Document("listDatabases", 1));
    }
}
