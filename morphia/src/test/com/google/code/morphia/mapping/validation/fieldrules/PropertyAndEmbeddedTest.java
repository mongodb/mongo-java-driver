/**
 * 
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.PreSave;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import com.mongodb.DBObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 *
 */
public class PropertyAndEmbeddedTest extends TestBase {
	public static class E extends TestEntity {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		@Embedded("myFunkyR")
		R r = new R();
		
		@PreSave
		public void preSave(DBObject o) {
			document = (String) ((DBObject) o.get("myFunkyR")).get("foo");
		}
		
		@Transient
		String document;
	}
	
	public static class E2 extends TestEntity {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		@Embedded
		@Property("myFunkyR")
		String s;
	}
	
	public static class R {
		String foo = "bar";
	}
	
	@Test
	public void testCheck() {
		
		E e = new E();
		ds.save(e);
		
		assertEquals("bar", e.document);
		
		new AssertedFailure(ConstraintViolationException.class) {
			public void thisMustFail() throws Throwable {
				morphia.map(E2.class);
			}
		};
	}
}
