package com.google.code.morphia.query;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.TestMapping.BaseEntity;
import com.google.code.morphia.annotations.Entity;

public class TestStringPatternQueries extends TestBase {
	@Entity
	static class E extends BaseEntity {
		final String name;
		
		public E(String name) {
			this.name = name;
		}
		
		protected E() {
			name = null;
		}
	}
	
	@Test
	public void testStartsWith() throws Exception {
		
		ds.save(new E("A"), new E("a"), new E("Ab"), new E("ab"), new E("c"));
		
		Assert.assertEquals(2, ds.createQuery(E.class).field("name").startsWith("a").countAll());
		Assert.assertEquals(4, ds.createQuery(E.class).field("name").startsWithIgnoreCase("a").countAll());
		Assert.assertEquals(4, ds.createQuery(E.class).field("name").startsWithIgnoreCase("A").countAll());
	}
	
	@Test
	public void testEndsWith() throws Exception {
		
		ds.save(new E("bxA"), new E("xba"), new E("xAb"), new E("xab"), new E("xcB"), new E("aba"));
		
		Assert.assertEquals(2, ds.createQuery(E.class).field("name").endsWith("b").countAll());
		Assert.assertEquals(3, ds.createQuery(E.class).field("name").endsWithIgnoreCase("b").countAll());
	}
	
	@Test
	public void testContains() throws Exception {
		
		ds.save(new E("xBA"), new E("xa"), new E("xAb"), new E("xab"), new E("xcB"), new E("aba"));
		
		Assert.assertEquals(3, ds.createQuery(E.class).field("name").contains("b").countAll());
		Assert.assertEquals(5, ds.createQuery(E.class).field("name").containsIgnoreCase("b").countAll());
	}

}
