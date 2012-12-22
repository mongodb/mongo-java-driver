package com.google.code.morphia.callbacks;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.TestBase;


public class TestProblematicPostPersistEntity extends TestBase{

	@Test
	public void testCallback() throws Exception {
		ProblematicPostPersistEntity p = new ProblematicPostPersistEntity();
		ds.save(p);
		Assert.assertTrue(p.called);
		Assert.assertTrue(p.i.called);
	}
}
