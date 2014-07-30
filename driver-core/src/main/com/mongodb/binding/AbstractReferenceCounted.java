package com.mongodb.binding;

import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractReferenceCounted implements ReferenceCounted {
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    @Override
    public int getCount() {
        return referenceCount.get();
    }

    @Override
    public ReferenceCounted retain() {
        if (referenceCount.incrementAndGet() == 1) {
            throw new IllegalStateException("Attempted to increment the reference count when it is already 0");
        }
        return this;
    }

    @Override
    public void release() {
        if (referenceCount.decrementAndGet() < 0) {
            throw new IllegalStateException("Attempted to decrement the reference count below 0");
        }
    }
}