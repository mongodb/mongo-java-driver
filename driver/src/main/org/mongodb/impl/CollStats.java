package org.mongodb.impl;

import org.mongodb.CommandDocument;
import org.mongodb.operation.MongoCommandOperation;

final class CollStats extends MongoCommandOperation {
    CollStats(final String collectionName) {
        super(new CommandDocument("collStats", collectionName));
    }
}
