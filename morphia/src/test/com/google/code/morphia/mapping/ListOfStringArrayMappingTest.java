/**
 * 
 */
package com.google.code.morphia.mapping;

import java.util.ArrayList;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;

/**
 * @author scotthernandez
 *
 */
public class ListOfStringArrayMappingTest extends TestBase {
	private static class ContainsListStringArray {
		@Id ObjectId id;
		ArrayList<String[]> listOfStrings = new ArrayList<String[]>();
	}
	

	@Test @Ignore //Add back when we figure out why we get this strange error :java.lang.ClassCastException: java.lang.String cannot be cast to [Ljava.lang.String;
	public void testMapping() throws Exception {
		morphia.map(ContainsListStringArray.class);
		ContainsListStringArray ent = new ContainsListStringArray();
		ent.listOfStrings.add(new String[] { "a", "b"});
		ds.save(ent);
		ContainsListStringArray loaded = ds.get(ent);
		Assert.assertNotNull(loaded.id);
		String[] arr = loaded.listOfStrings.get(0);
		String a = arr[0];
		String b = arr[1];
		Assert.assertEquals("a", a);
		Assert.assertEquals("b", b);

	}
	
	
}
