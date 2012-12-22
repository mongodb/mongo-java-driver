/**
 * 
 */
package com.google.code.morphia.mapping;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ConcreteClassEmbeddedOverrrideTest extends TestBase {
	
	public static class E {
		@Id ObjectId id;

		@Embedded
		A a1 = new A();
		
		@Embedded(concreteClass = B.class)
		A a2 = new A();
	}
	
	public static class A {
		String s = "A";
	}
	
	public static class B extends A {
		public B() {
			s = "B";
		}
	}
	
	@Test
	public void test() throws Exception {
		E e1 = new E();
		Assert.assertEquals("A", e1.a1.s);
		Assert.assertEquals("A", e1.a2.s);
		
		ds.save(e1);
		
		E e2 = ds.get(e1);
		
		Assert.assertEquals("A", e2.a1.s);
		Assert.assertEquals("A", e2.a2.s);
		Assert.assertEquals(B.class, e2.a2.getClass());
		Assert.assertEquals(A.class, e2.a1.getClass());
		
	}
}
