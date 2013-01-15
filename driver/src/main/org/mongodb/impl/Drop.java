package org.mongodb.impl;

import org.mongodb.CommandDocument;
import org.mongodb.operation.MongoCommandOperation;

/**
 * Knows how to build the document that represents the Drop Collection command.
 */
public class Drop extends MongoCommandOperation {
    public Drop(final String collectionName) {
        super(new CommandDocument("drop", collectionName));
    }
}
