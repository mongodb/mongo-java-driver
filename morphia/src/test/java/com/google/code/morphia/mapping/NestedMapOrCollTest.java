/**
 * 
 */
package com.google.code.morphia.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;

/**
 * @author scotthernandez
 */
public class NestedMapOrCollTest extends TestBase{
	

	@SuppressWarnings({"rawtypes", "unchecked", "unused"})
	private static class HasMapOfMap {
		@Id ObjectId id;

		@Embedded
		Map<String, Map<String, String>> mom = new HashMap();
	}

	@SuppressWarnings({"rawtypes", "unchecked", "unused"})
	private static class HasMapOfList {
		@Id ObjectId id;

		@Embedded
		Map<String, List<String>> mol = new HashMap();
	}

	@SuppressWarnings({"rawtypes", "unchecked", "unused"})
	private static class HasMapOfListOfMapMap {
		@Id ObjectId id;

		@Embedded
		Map<String, List<NestedMapOrCollTest.HasMapOfMap>> mol = new HashMap();
	}
	
	@Test
	public void testMapOfMap() throws Exception {
		HasMapOfMap hmom = new HasMapOfMap();
		Map<String, String> dmap = new HashMap<String, String>();
		hmom.mom.put("root", dmap);
		dmap.put("deep", "values");
		dmap.put("peer", "lame");
		
		ds.save(hmom);
		hmom = ds.find(HasMapOfMap.class).get();
		Assert.assertNotNull(hmom.mom);
		Assert.assertNotNull(hmom.mom.get("root"));
		Assert.assertNotNull(hmom.mom.get("root").get("deep"));
		Assert.assertEquals("values", hmom.mom.get("root").get("deep"));
		Assert.assertNotNull("lame", hmom.mom.get("root").get("peer"));
	}

	@Test
	public void testMapOfList() throws Exception {
		HasMapOfList hmol = new HasMapOfList();
		hmol.mol.put("entry1", Collections.singletonList("val1"));
		hmol.mol.put("entry2", Collections.singletonList("val2"));
		
		ds.save(hmol);
		hmol = ds.find(HasMapOfList.class).get();
		Assert.assertNotNull(hmol.mol);
		Assert.assertNotNull(hmol.mol.get("entry1"));
		Assert.assertNotNull(hmol.mol.get("entry1").get(0));
		Assert.assertEquals("val1", hmol.mol.get("entry1").get(0));
		Assert.assertNotNull("val2", hmol.mol.get("entry2").get(0));
	}

	@Test
	public void testMapOfListOfMapMap() throws Exception {
		HasMapOfMap hmom = new HasMapOfMap();
		Map<String, String> dmap = new HashMap<String, String>();
		hmom.mom.put("root", dmap);
		dmap.put("deep", "values");
		dmap.put("peer", "lame");

		
		HasMapOfListOfMapMap hmolomm = new HasMapOfListOfMapMap();
		hmolomm.mol.put("r1", Collections.singletonList(hmom));
		hmolomm.mol.put("r2", Collections.singletonList(hmom));
		
		ds.save(hmolomm);
		hmolomm = ds.find(HasMapOfListOfMapMap.class).get();
		Assert.assertNotNull(hmolomm.mol);
		Assert.assertNotNull(hmolomm.mol.get("r1"));
		Assert.assertNotNull(hmolomm.mol.get("r1").get(0));
		Assert.assertNotNull(hmolomm.mol.get("r1").get(0).mom);
		Assert.assertEquals("values", hmolomm.mol.get("r1").get(0).mom.get("root").get("deep"));
		Assert.assertEquals("lame", hmolomm.mol.get("r1").get(0).mom.get("root").get("peer"));
		Assert.assertEquals("values", hmolomm.mol.get("r2").get(0).mom.get("root").get("deep"));
		Assert.assertEquals("lame", hmolomm.mol.get("r2").get(0).mom.get("root").get("peer"));
	}
}
