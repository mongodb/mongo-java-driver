package com.google.code.morphia.mapping.lazy;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;

@SuppressWarnings("unchecked")
public class TestReferenceCollection extends ProxyTestBase {
	@Test
	public final void testCreateProxy() {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled())
        {
            return;
        }

		A a = new A();
		B b1 = new B();
		B b2 = new B();
		
		a.bs.add(b1);
		a.bs.add(b2);
		
		Collection<B> lazyBs = a.lazyBs;
		lazyBs.add(b1);
		lazyBs.add(b2);
		
		ds.save(b2, b1, a);
		
		a = ds.get(a);
		
		lazyBs = a.lazyBs;
		Assert.assertNotNull(lazyBs);
		assertNotFetched(lazyBs);
		
		Assert.assertNotNull(lazyBs.iterator().next());
		assertFetched(lazyBs);
		
		a = deserialize(a);
		
		lazyBs = a.lazyBs;
		Assert.assertNotNull(lazyBs);
		assertNotFetched(lazyBs);
		
		Assert.assertNotNull(lazyBs.iterator().next());
		assertFetched(lazyBs);
		
		a = deserialize(a);
		
		ds.save(a);
		lazyBs = a.lazyBs;
		assertNotFetched(lazyBs);
	}
	
	@Test
	public void testOrderingPreserved() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled())
        {
            return;
        }

		A a = new A();
		B b1 = new B();
		b1.setFoo("b1");
		a.lazyBs.add(b1);
		
		B b2 = new B();
		b2.setFoo("b2");
		a.lazyBs.add(b2);
		ds.save(b1);
		ds.save(b2);
		
		Assert.assertEquals("b1", a.lazyBs.iterator().next().foo);
		
		ds.save(a);

		A reloaded = ds.get(a);
		Assert.assertEquals("b1", reloaded.lazyBs.iterator().next().foo);
		Collections.swap((List<B>) reloaded.lazyBs, 0, 1);
		Assert.assertEquals("b2", reloaded.lazyBs.iterator().next().foo);
		
		ds.save(reloaded);

		reloaded = ds.get(reloaded);
		Collection<B> lbs = reloaded.lazyBs;
		Assert.assertEquals(2, lbs.size());
		Iterator<B> iterator = lbs.iterator();
		Assert.assertEquals("b2", iterator.next().foo);
		
	}
	
	public static class A extends TestEntity {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Reference(lazy = false)
		Collection<B> bs = new LinkedList();

		@Reference(lazy = true)
		Collection<B> lazyBs = new LinkedList();
		
	}
	
	public static class B extends TestEntity {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String foo;
		
		public void setFoo(final String string) {
			this.foo = string;
		}
		
		public String getFoo() {
			return this.foo;
		}

		@Override
		public String toString() {
			return super.toString() + " : id = " + id + ", foo=" + foo;
		}
	}

}
