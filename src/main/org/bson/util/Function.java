package org.bson.util;

interface Function<A, B> {
    B apply(A a);
}
