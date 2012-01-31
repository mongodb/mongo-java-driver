package org.bson.util.concurrent;

public interface Function<A, B> {
    B apply(A a);
}
