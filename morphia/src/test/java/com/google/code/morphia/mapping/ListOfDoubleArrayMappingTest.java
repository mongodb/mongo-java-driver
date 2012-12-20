/**
 * 
 */
package com.google.code.morphia.mapping;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;

/**
 * @author scotthernandez
 *
 */
public class ListOfDoubleArrayMappingTest extends TestBase {
	private static class ContainsListDoubleArray {
		@Id ObjectId id;
		List<Double[]> points = new ArrayList<Double[]>();
	}
	

	@Test
	public void testMapping() throws Exception {
		morphia.map(ContainsListDoubleArray.class);
		ContainsListDoubleArray ent = new ContainsListDoubleArray();
		ent.points.add(new Double[] { 1.1, 2.2});
		ds.save(ent);
		ContainsListDoubleArray loaded = ds.get(ent);
		Assert.assertNotNull(loaded.id);
//		Assert.assertEquals(1.1D, loaded.points.get(0)[0], 0);
//		Assert.assertEquals(2.2D, loaded.points.get(0)[1], 0);
		
	}
	
	
}
