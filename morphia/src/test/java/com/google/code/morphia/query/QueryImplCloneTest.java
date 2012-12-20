/**
 * 
 */
package com.google.code.morphia.query;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;

/**
 * @author doc
 * 
 */
public class QueryImplCloneTest extends TestBase {
	private static final List<String> AllowedChangingFields = Arrays.asList(new String[] { "cache", "query" });

	private boolean sameState(Query q1, Query q2) throws IllegalArgumentException, IllegalAccessException {
		return sameState(q1.getClass(), q1, q2);
	}
	
	private boolean sameState(Class c, Query q1, Query q2) throws IllegalArgumentException, IllegalAccessException {

		Field[] fields = c.getDeclaredFields();
		for (Field f : fields) {
			f.setAccessible(true);
			
			Object v1 = f.get(q1);
			Object v2 = f.get(q2);
			
			System.out.println("checking field " + f.getName() + " v1=" + v1 + " v2=" + v2);
			
			if (v1 == null && v2 == null)
				continue;
			
			if (v1 != null && v1.equals(v2))
				continue;
			
			if (AllowedChangingFields.contains(f.getName()))
				continue;
			else
				throw new RuntimeException(f.getName() + " v1=" + v1 + " v2=" + v2);
		}
		
		Class superclass = c.getSuperclass();
		return (superclass == null || sameState(superclass, q1, q2));
	}
	
	static class E1 {
		@Id
		ObjectId id;
		
		String a;
		String b;
		int i;
		E2 e2 = new E2();
	}
	
	static class E2 {
		String foo;
	}
	
	@Test
	public void testQueryClone() throws Exception {
		Query q = ds.createQuery(E1.class).field("i").equal(5).limit(5).filter("a", "value_a").filter("b", "value_b")
				.skip(5).batchSize(10).disableCursorTimeout().hintIndex("a")
				.order("a");
		q.disableValidation().filter("foo", "bar");
		Assert.assertTrue(sameState(q, q.clone()));
	}
}
