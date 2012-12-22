package com.google.code.morphia.largeObjectsWithCursor;

import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.TestMapping.BaseEntity;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.query.Query;

/** Test from list, but doesn't seems to be a problem. Here as an example. */
public class TestStuff extends TestBase {
	private int documentsNb;
	
	@Entity
	static class E extends BaseEntity {
		private static final long serialVersionUID = 1L;
		protected final Integer index;
		protected final byte[] largeContent;
		
		public E() {
			index = null;
			largeContent = null;
		}
		
		private byte[] createLargeByteArray() {
			int size = (int) (4000 + Math.random() * 100000);
			byte[] arr = new byte[size];
			for (int i = 0; i < arr.length; i++) {
				arr[i] = 'a';
			}
			return arr;
		}
		
		public E(int i) {
			this.index = i;
			largeContent = createLargeByteArray();
		}
		
	}
	
	@Override
	@Before
	public void setUp() {
//		try {
//			this.mongo = new Mongo("stump");
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}

		super.setUp();
		morphia.map(E.class);
		documentsNb = 1000;
		for (int i = 0; i < documentsNb; i++) {
			ds.save(new E(i));
		}
	}
	
	@Test
	public void testWithManyElementsInCollection() throws Exception {
		Query<E> query = ds.createQuery(E.class);
		long countAll = query.countAll();
		query = ds.createQuery(E.class);
		List<E> list = query.asList();
		Assert.assertEquals(documentsNb, countAll);
		Assert.assertEquals(documentsNb, list.size());
	}
}