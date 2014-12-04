package com.mongodb;

/**
 * An exception indicating that the server is a member of a replica set but is in recovery mode, and therefore refused to execute
 * the operation. This can happen when a server is starting up and trying to join the replica set.
 *
 * @since 3.0
 */
public class MongoNodeIsRecoveringException extends MongoServerException {
    private static final long serialVersionUID = 6062524147327071635L;

    /**
     * Construct an instance.
     *
     * @param serverAddress the address of the server
     */
    public MongoNodeIsRecoveringException(final ServerAddress serverAddress) {
        super("The server is in recovery mode and did not execute the operation", serverAddress);
    }
}
