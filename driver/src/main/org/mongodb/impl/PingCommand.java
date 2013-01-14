package org.mongodb.impl;

import org.mongodb.CommandDocument;
import org.mongodb.operation.MongoCommandOperation;

final class PingCommand extends MongoCommandOperation {
    PingCommand() {
        super(new CommandDocument("ping", 1));
    }
}
