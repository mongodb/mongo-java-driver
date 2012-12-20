/**
 * 
 */
package com.google.code.morphia.utils;

import org.junit.Test;

import com.google.code.morphia.testutil.AssertedFailure;

public class FieldNameTest {
	
	private String foo;
	private String bar;

	@Test
	public void testFieldNameOf() throws Exception {
		String name = "foo";
		junit.framework.Assert.assertEquals("foo", FieldName.of("foo"));
		junit.framework.Assert.assertEquals("bar", FieldName.of("bar"));
		new AssertedFailure(FieldName.FieldNameNotFoundException.class) {
			
			@Override
			protected void thisMustFail() throws Throwable {
				FieldName.of("buh");
			}
		};
		junit.framework.Assert.assertEquals("x", FieldName.of(E2.class, "x"));
		junit.framework.Assert.assertEquals("y", FieldName.of(E2.class, "y"));
	}
}

class E1 {
	private final int x = 0;
}

class E2 extends E1 {
	private final int y = 0;
}
