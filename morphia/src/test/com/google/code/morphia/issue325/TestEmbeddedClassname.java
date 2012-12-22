package com.google.code.morphia.issue325;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;


/**
 */
@SuppressWarnings("unused")
public class TestEmbeddedClassname extends TestBase {
	
//	@SuppressWarnings("unused")
	@Entity(noClassnameStored = true)
	private static class Root {
	    @Id String id = "a";

	    @Embedded
	    List<A> as = new ArrayList<A>();

	    @Embedded
	    List<B> bs = new ArrayList<B>();
	}
	
	private static class A {
		String name = "undefined";
		
		@Transient DBObject raw;
		@PreLoad
		void preLoad(DBObject dbObj) {
			raw = dbObj;
		}
	}
	
	private static class B extends A{
		String description = "<descr. here>";
	}

	@Test
	public final void testEmbeddedClassname() {
		Root r = new Root();
		ds.save(r);
		
		A a = new A();
		ds.update(ds.createQuery(Root.class), ds.createUpdateOperations(Root.class).add("as", a));
		r = ds.get(Root.class, "a");
		Assert.assertFalse(r.as.get(0).raw.containsField(Mapper.CLASS_NAME_FIELDNAME));

		B b = new B();
		ds.update(ds.createQuery(Root.class), ds.createUpdateOperations(Root.class).add("bs", b));
		r = ds.get(Root.class, "a");
		Assert.assertFalse(r.bs.get(0).raw.containsField(Mapper.CLASS_NAME_FIELDNAME));
		
		ds.delete(ds.createQuery(Root.class));
		//test saving an B in as, and it should have the classname.
		
		ds.save(new Root());
		b = new B();
		ds.update(ds.createQuery(Root.class), ds.createUpdateOperations(Root.class).add("as", b));
		r = ds.get(Root.class, "a");
		Assert.assertTrue(r.as.get(0).raw.containsField(Mapper.CLASS_NAME_FIELDNAME));
		
	}

}
