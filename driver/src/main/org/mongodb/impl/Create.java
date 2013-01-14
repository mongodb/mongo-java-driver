package org.mongodb.impl;

import org.mongodb.CreateCollectionOptions;
import org.mongodb.operation.MongoCommandOperation;

final class Create extends MongoCommandOperation {
    public Create(final CreateCollectionOptions createCollectionOptions) {
        super(createCollectionOptions.asCommandDocument());
    }
}
