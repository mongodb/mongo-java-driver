package com.google.code.morphia.mapping.lazy;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.Key;
import com.google.code.morphia.testutil.TestEntity;

public class TestCGLibLazyProxyFactory extends ProxyTestBase
 {
	@Test
	public final void testCreateProxy()
	{
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled())
        {
            return;
        }

		final E e = new E();
		e.setFoo("bar");
		final Key<E> key = ds.save(e);
		E eProxy = new CGLibLazyProxyFactory().createProxy(E.class, key,
				new DefaultDatastoreProvider());

		assertNotFetched(eProxy);
		Assert.assertEquals("bar", eProxy.getFoo());
		assertFetched(eProxy);

		eProxy = deserialize(eProxy);
		assertNotFetched(eProxy);
		Assert.assertEquals("bar", eProxy.getFoo());
		assertFetched(eProxy);

	}
	public static class E extends TestEntity
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String foo;

		public void setFoo(final String string)
		{
			foo = string;
		}

		public String getFoo()
		{
			return foo;
		}
	}

}
