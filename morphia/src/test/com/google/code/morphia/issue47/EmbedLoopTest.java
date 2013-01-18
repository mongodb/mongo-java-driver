/**
 *
 */
package com.google.code.morphia.issue47;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class EmbedLoopTest extends TestBase {

    @Entity
    static class A extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Embedded
        private B b;
    }

    @Embedded
    static class B extends TestEntity {
        private static final long serialVersionUID = 1L;
        private final String someProperty = "someThing";

        // produces stackoverflow, might be detectable?
        // @Reference this would be right way to do it.

        @Embedded
        private A a;
    }

    @Test
    @Ignore
    public void testCircularRefs() throws Exception {

        morphia.map(A.class);

        A a = new A();
        a.b = new B();
        a.b.a = a;

        Assert.assertSame(a, a.b.a);

        this.ds.save(a);
        a = this.ds.find(A.class, "_id", a.getId()).get();
        Assert.assertSame(a, a.b.a);
    }
}