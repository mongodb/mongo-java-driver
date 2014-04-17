package org.mongodb.binding;


/**
 *
 */
public interface ReadWriteBinding extends ReadBinding, WriteBinding, ReferenceCounted {
    @Override
    ReadWriteBinding retain();
}
