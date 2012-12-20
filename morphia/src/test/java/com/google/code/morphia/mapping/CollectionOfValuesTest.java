/**
 * 
 */
package com.google.code.morphia.mapping;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import com.google.code.morphia.Key;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 *
 */
public class CollectionOfValuesTest extends TestBase {
	
	public static class ContainsListOfList {
		@Id ObjectId id;
		@Embedded List<List<String>> listOfList;
	}
	
	public static class ContainsTwoDimensionalArray {
		@Id
		String id;
		@Embedded
		byte[][] twoDimArray;
	}
	
	@Test
	@Ignore("Not yet implemented")
	public void testTwoDimensionalArrayMapping() throws Exception {
		morphia.map(ContainsTwoDimensionalArray.class);
		ContainsTwoDimensionalArray entity = new ContainsTwoDimensionalArray();
		byte[][] test2DimBa = new byte[][] { "Joseph".getBytes(), "uwe".getBytes() };
		entity.twoDimArray = test2DimBa;
		Key<ContainsTwoDimensionalArray> savedKey = ds.save(entity);
		ContainsTwoDimensionalArray loaded = ds.get(ContainsTwoDimensionalArray.class, savedKey.getId());
		Assert.assertNotNull(loaded.twoDimArray);
		Assert.assertEquals(test2DimBa, loaded.twoDimArray);
		Assert.assertNotNull(loaded.id);
	}

	@Test
	public void testListOfListMapping() throws Exception {
		morphia.map(ContainsListOfList.class);
		ds.delete(ds.find(ContainsListOfList.class));
		ContainsListOfList entity = new ContainsListOfList();
		ArrayList<List<String>> testList = new ArrayList<List<String>>();
		ArrayList<String> element1 = new ArrayList<String>();
		element1.add("element1");
		testList.add(element1);

		List<String> element2 = new ArrayList<String>();
		element2.add("element2");
		testList.add(element2);

		entity.listOfList = testList;
		ds.save(entity);
		ContainsListOfList loaded = ds.get(entity);

		Assert.assertNotNull(loaded.listOfList);

		Assert.assertEquals(testList, loaded.listOfList);
		List<String> loadedElement1 = loaded.listOfList.get(0);
		Assert.assertEquals(element1, loadedElement1);
		Assert.assertEquals(element1.get(0), loadedElement1.get(0));
		Assert.assertNotNull(loaded.id);
	}
	
}
