package com.mongodb;

/**
 * A base class for exceptions indicating a failure condition within the driver.
 */
public class MongoClientException extends MongoInternalException {

    private static final long serialVersionUID = -5127414714432646066L;

    /**
     * Constructs a new instance with the given message.
     *
     * @param msg the message
     */
    MongoClientException(String msg) {
        super(msg);
    }
}
