package org.mongodb.impl;

import org.mongodb.CommandDocument;
import org.mongodb.operation.MongoCommandOperation;

final class DropDatabase extends MongoCommandOperation {
    DropDatabase() {
        super(new CommandDocument("dropDatabase", 1));
    }
}
