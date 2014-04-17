package org.mongodb.binding;

/**
 * An interface for reference-counted objects.
 *
 * @since 3.0
 */
public interface ReferenceCounted {
    /**
     * Gets the current reference count, which starts at 0.
     *
     * @return the current count, which must be >= 0
     */
    int getCount();

    /**
     * Retain an additional reference to this object.  All retained references must be released, or there will be a leak.
     *
     * @return this
     */
    ReferenceCounted retain();

    /**
     * Release a reference to this object.
     * @throws java.lang.IllegalStateException if the reference count is already 0
     */
    void release();
}