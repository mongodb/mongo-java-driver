package com.google.code.morphia.callbacks;

import org.bson.types.ObjectId;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PostPersist;

public class ProblematicPostPersistEntity {
	@Id
	ObjectId id;

	Inner i = new Inner();

	boolean called;

	@PostPersist
	void m1() {
		called = true;
	}

	static class Inner {
		boolean called;

		String foo = "foo";

		@PostPersist
		void m2() {
			called = true;
		}
	}
}
