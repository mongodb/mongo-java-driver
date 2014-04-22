package org.mongodb.binding;

import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractReferenceCounted implements ReferenceCounted {
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    @Override
    public int getCount() {
        return referenceCount.get();
    }

    @Override
    public ReferenceCounted retain() {
        referenceCount.incrementAndGet();
        return this;
    }

    @Override
    public void release() {
        if (referenceCount.decrementAndGet() < 0) {
            throw new IllegalStateException("Attempted to decrement the reference count below 0");
        }
    }
}