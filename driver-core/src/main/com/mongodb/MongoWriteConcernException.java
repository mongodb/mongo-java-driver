package com.mongodb;

import com.mongodb.bulk.WriteConcernError;

/**
 * An exception indicating a failure to apply the write concern to the requested write operation
 *
 * @see com.mongodb.WriteConcern
 *
 * @since 3.0
 */
public class MongoWriteConcernException extends MongoServerException {
    private static final long serialVersionUID = 4577579466973523211L;

    private final WriteConcernError writeConcernError;

    /**
     * Construct an instance.
     *
     * @param writeConcernError the write concern error
     * @param serverAddress the server address
     */
    public MongoWriteConcernException(final WriteConcernError writeConcernError, final ServerAddress serverAddress) {
        super(writeConcernError.getCode(), writeConcernError.getMessage(), serverAddress);
        this.writeConcernError = writeConcernError;
    }

    /**
     *
     * @return the write concern error
     */
    public WriteConcernError getWriteConcernError() {
        return writeConcernError;
    }
}
