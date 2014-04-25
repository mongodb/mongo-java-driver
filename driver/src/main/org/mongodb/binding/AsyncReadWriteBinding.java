package org.mongodb.binding;


/**
 * An asynchronous factory of connection sources to servers that can be read from or written to.
 *
 * @since 3.0
 */
public interface AsyncReadWriteBinding extends AsyncReadBinding, AsyncWriteBinding, ReferenceCounted {
    @Override
    AsyncReadWriteBinding retain();
}
