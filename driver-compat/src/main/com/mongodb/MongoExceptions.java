package com.mongodb;

import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.command.MongoDuplicateKeyException;

public class MongoExceptions {
    public static com.mongodb.MongoException mapException(final org.mongodb.MongoException e) {
        if (e instanceof MongoDuplicateKeyException) {
            return new MongoException.DuplicateKey((MongoDuplicateKeyException) e);
        } else if (e instanceof MongoCommandFailureException) {
            return new CommandFailureException(new CommandResult(((MongoCommandFailureException) e).getCommandResult()));
        }

        return new MongoException(e.getMessage(), e.getCause());
    }
}
