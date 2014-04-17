package org.mongodb.binding;

import org.mongodb.connection.Connection;

/**
 * A source of connections to a single MongoDB server.
 *
 * @since 3.0
 */
public interface ConnectionSource extends ReferenceCounted {
    /**
     * Gets a connection from this source.
     * @return the connection
     */
    Connection getConnection();

    @Override
    ConnectionSource retain();
}
