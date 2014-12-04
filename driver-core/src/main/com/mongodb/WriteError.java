package com.mongodb;

import com.mongodb.bulk.BulkWriteError;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Represents the details of a write error , e.g. a duplicate key error
 *
 * @since 3.0
 */
public class WriteError {
    private final int code;
    private final String message;
    private final BsonDocument details;

    /**
     * Constructs a new instance.
     *
     * @param code    the error code
     * @param message the error message
     * @param details details about the error
     */
    public WriteError(final int code, final String message, final BsonDocument details) {
        this.code = code;
        this.message = notNull("message", message);
        this.details = notNull("details", details);
    }

    /**
     * Gets the code associated with this error.
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the message associated with this error.
     *
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets the details associated with this error.  This document will not be null, but may be empty.
     *
     * @return the details
     */
    public BsonDocument getDetails() {
        return details;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WriteError that = (WriteError) o;

        if (code != that.code) {
            return false;
        }
        if (!details.equals(that.details)) {
            return false;
        }
        if (!message.equals(that.message)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = code;
        result = 31 * result + message.hashCode();
        result = 31 * result + details.hashCode();
        return result;
    }
}
