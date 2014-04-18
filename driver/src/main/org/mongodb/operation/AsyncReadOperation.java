package org.mongodb.operation;

import org.mongodb.MongoFuture;
import org.mongodb.binding.AsyncReadBinding;

/**
 * An operation which asynchronously reads from a MongoDB server.
 *
 * @param <T> the return type of the execute method
 *
 * @since 3.0
 */
public interface AsyncReadOperation<T> {
    /**
     * General execute which can return anything of type T
     *
     * @param binding the binding to execute in the context of
     * @return a future for the result
     */
    MongoFuture<T> executeAsync(AsyncReadBinding binding);
}