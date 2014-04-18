package org.mongodb.binding;

import org.mongodb.MongoFuture;
import org.mongodb.binding.ReferenceCounted;
import org.mongodb.connection.Connection;

/**
 * A source of connections to a single MongoDB server.
 *
 * @since 3.0
 */
public interface AsyncConnectionSource extends ReferenceCounted {
    /**
     * Gets a connection from this source.
     * @return the connection
     */
    MongoFuture<Connection> getConnection();

    @Override
    AsyncConnectionSource retain();
}
