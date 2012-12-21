package org.mongodb.impl;

import org.mongodb.MongoException;
import org.mongodb.result.CommandResult;

public class ErrorHandling {
    static void handleErrors(final CommandResult commandResult, final String message) {
        Object serverErrorMessage = commandResult.getDocument().get("errmsg");
        if (serverErrorMessage != null) {
            throw new MongoException(message + ": " + serverErrorMessage);
        }
    }
}
