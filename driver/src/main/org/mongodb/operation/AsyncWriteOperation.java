package org.mongodb.operation;

import org.mongodb.MongoFuture;
import org.mongodb.binding.AsyncWriteBinding;

/**
 * An operation which asynchronously writes to a MongoDB server.
 *
 * @param <T> the return type of the execute method
 *
 * @since 3.0
 */
public interface AsyncWriteOperation<T> {
    /**
     * General execute which can return anything of type T
     *
     * @param binding the binding to execute in the context of
     * @return a future for the result
     */
    MongoFuture<T> executeAsync(AsyncWriteBinding binding);
}
