package org.mongodb.binding;


/**
 * A factory of connection sources to servers that can be read from or written to.
 *
 * @since 3.0
 */
public interface ReadWriteBinding extends ReadBinding, WriteBinding, ReferenceCounted {
    @Override
    ReadWriteBinding retain();
}
