/**
 * 
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.PreSave;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.testutil.TestEntity;
import com.mongodb.DBObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 *
 */
public class SerializedNameTest extends TestBase {
	public static class E extends TestEntity {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		@Serialized("changedName")
		byte[] b = "foo".getBytes();
		
		@PreSave
		public void preSave(DBObject o) {
			document = new String((byte[]) o.get("changedName"));
		}
		
		@Transient
		String document;
	}
	
	@Test
	public void testCheck() {
		
		E e = new E();
		ds.save(e);
		
		assertEquals("foo", e.document);

	}
}
