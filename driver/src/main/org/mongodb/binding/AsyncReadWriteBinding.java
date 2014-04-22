package org.mongodb.binding;


/**
 *
 */
public interface AsyncReadWriteBinding extends AsyncReadBinding, AsyncWriteBinding, ReferenceCounted {
    @Override
    AsyncReadWriteBinding retain();
}
