package com.mongodb;

/**
 * An exception indicating the failure of a write operation.
 *
 * @since 3.0
 */
public class MongoWriteException extends MongoServerException {

    private static final long serialVersionUID = -1906795074458258147L;

    private final WriteError error;

    /**
     * Construct an instance
     * @param error the error
     * @param serverAddress the server address
     */
    public MongoWriteException(final WriteError error, final ServerAddress serverAddress) {
        super(error.getCode(), error.getMessage(), serverAddress);
        this.error = error;
    }

    /**
     * Gets the error.
     *
     * @return the error
     */
    public WriteError getError() {
        return error;
    }
}
