/**
 * 
 */
package com.google.code.morphia.issue45;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.testutil.TestEntity;

public class TestEmptyEntityMapping extends TestBase
{
    @Entity
    static class A extends TestEntity{
    	private static final long serialVersionUID = 1L;
		@Embedded
		B b;
    }

    @Embedded
    static class B
    {
        @Transient
        String foo;
    }

    @Test
    public void testEmptyEmbeddedNotNullAfterReload() throws Exception
    {
        A a = new A();
        a.b = new B();

        this.ds.save(a);
        Assert.assertNotNull(a.b);

        a = this.ds.find(A.class, "_id", a.getId()).get();
        Assert.assertNull(a.b);
    }
}
