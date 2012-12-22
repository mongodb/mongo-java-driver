package com.google.code.morphia.callbacks;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PostLoad;
import com.google.code.morphia.annotations.PostPersist;
import com.google.code.morphia.annotations.Transient;

public class TestMultipleCallbacksPerMethod extends TestBase {
	static abstract class CallbackAbstractEntity {
		@Id
		private String _id = new ObjectId().toStringMongod();
		
		public String getId() {
			return _id;
		}
		
		@Transient
		private boolean persistentMarker = false;
		
		public boolean isPersistent() {
			return persistentMarker;
		}
		
		@PostPersist
		@PostLoad
		void markPersitent() {
			persistentMarker = true;
		}
	}
	
	static class SomeEntity extends CallbackAbstractEntity {
		
	}
	
	@Test
	public void testMultipleCallbackAnnotation() throws Exception {
		SomeEntity entity = new SomeEntity();
		Assert.assertFalse(entity.isPersistent());
		ds.save(entity);
		Assert.assertTrue(entity.isPersistent());
		SomeEntity reloaded = ds.find(SomeEntity.class, "_id", entity.getId()).get();
		Assert.assertTrue(reloaded.isPersistent());
	}
}