package com.mongodb;

/**
 * An exception indicating that the server is a member of a replica set but is not the primary, and therefore refused to execute either a
 * write operation or a read operation that required a primary.  This can happen during a replica set election.
 *
 * @since 3.0
 */
public class MongoNotPrimaryException extends MongoServerException {
    private static final long serialVersionUID = 694876345217027108L;

    /**
     * Construct an instance.
     *
     * @param serverAddress the address of the server
     */
    public MongoNotPrimaryException(final ServerAddress serverAddress) {
        super("The server is not the primary and did not execute the operation", serverAddress);
    }
}
