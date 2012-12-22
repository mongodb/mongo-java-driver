/**
 * 
 */
package com.google.code.morphia.mapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public class LocaleMappingTest extends TestBase {
	
	public static class E {
		@Id ObjectId id;
		Locale l1;
		
		@Embedded
		List<Locale> l2 = new ArrayList<Locale>();
		
		Locale[] l3;
	}
	
	@Test
	public void testLocaleMapping() throws Exception {
		E e = new E();
		e.l1 = Locale.CANADA_FRENCH;
		e.l2 = Arrays.asList(Locale.GERMANY, Locale.TRADITIONAL_CHINESE);
		e.l3 = new Locale[] { Locale.TRADITIONAL_CHINESE, Locale.FRENCH };
		
		ds.save(e);
		e = ds.get(e);
		
		Assert.assertEquals(Locale.CANADA_FRENCH, e.l1);
		
		Assert.assertEquals(2, e.l2.size());
		Assert.assertEquals(Locale.GERMANY, e.l2.get(0));
		Assert.assertEquals(Locale.TRADITIONAL_CHINESE, e.l2.get(1));
		
		Assert.assertEquals(2, e.l3.length);
		Assert.assertEquals(Locale.TRADITIONAL_CHINESE, e.l3[0]);
		Assert.assertEquals(Locale.FRENCH, e.l3[1]);

	}
}
