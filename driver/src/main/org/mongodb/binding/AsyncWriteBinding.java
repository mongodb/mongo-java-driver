package org.mongodb.binding;

import org.mongodb.MongoFuture;

/**
 * A factory of connection sources to servers that can be written to, e.g, a standalone, a mongos, or a replica set primary.
 *
 * @since 3.0
 */
public interface AsyncWriteBinding extends ReferenceCounted {
    /**
     * Supply a connection source to a server that can be written to
     *
     * @return a connection source
     */
    MongoFuture<AsyncConnectionSource> getWriteConnectionSource();

    @Override
    AsyncWriteBinding retain();
}
