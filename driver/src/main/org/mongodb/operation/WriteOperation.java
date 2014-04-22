package org.mongodb.operation;

import org.mongodb.binding.WriteBinding;

/**
 * An operation which writes to a MongoDB server.
 *
 * @param <T> the return type of the execute method
 *
 * @since 3.0
 */
public interface WriteOperation<T> {
    /**
     * General execute which can return anything of type T
     *
     * @param binding the binding to execute in the context of
     * @return T, the result of the execution
     */
    T execute(WriteBinding binding);
}
