package org.mongodb.operation;

import org.mongodb.binding.ReadBinding;

/**
 * An operation that reads from a MongoDB server.
 *
 * @param <T> the return type of the execute method
 *
 * @since 3.0
 */
public interface ReadOperation<T> {
    /**
     * General execute which can return anything of type T
     *
     * @param binding the binding to execute in the context of
     * @return T, the result of the execution
     */
    T execute(ReadBinding binding);
}