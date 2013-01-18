package com.google.code.morphia.callbacks;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PostPersist;
import org.bson.types.ObjectId;

public class ProblematicPostPersistEntity {
    @Id
    private ObjectId id;

    final Inner i = new Inner();

    boolean called;

    @PostPersist
    void m1() {
        called = true;
    }

    static class Inner {
        boolean called;

        private final String foo = "foo";

        @PostPersist
        void m2() {
            called = true;
        }
    }
}
