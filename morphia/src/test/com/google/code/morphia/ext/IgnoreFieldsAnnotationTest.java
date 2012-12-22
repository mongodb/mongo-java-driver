/**
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia.ext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.code.morphia.DatastoreImpl;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;

/**
 *
 * @author Scott Hernandez
 */
public class IgnoreFieldsAnnotationTest extends TestBase {

	public IgnoreFieldsAnnotationTest () {
		super();
	}
	
	Transient transAnn;
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.TYPE})
	static @interface IgnoreFields {
		String value();
	}
	
	@Entity
	@IgnoreFields("ignored")
	static class User {
		@Id ObjectId id;
		String email;
		String ignored = "never, never";
	}

	@Before
	public void setUp() {
		super.setUp();
		MappedClass.interestingAnnotations.add(IgnoreFields.class);
		this.morphia.map(User.class);
		processIgnoreFieldsAnnotations();
	}
	
	//remove any MappedField specified in @IngoreFields on the class.
	void processIgnoreFieldsAnnotations(){
		DatastoreImpl dsi = (DatastoreImpl) ds;
		for(MappedClass mc : dsi.getMapper().getMappedClasses()) {
			IgnoreFields ignores = (IgnoreFields) mc.getAnnotation(IgnoreFields.class);
			if (ignores != null) {
				for(String field : ignores.value().split(",")) {
					MappedField mf = mc.getMappedFieldByJavaField(field);
					mc.getPersistenceFields().remove(mf);
				}
			}
		}
	}
	@Test
	public void testIt() {
		User u = new User();
		u.email = "ScottHernandez@gmail.com";
		u.ignored = "test";
		ds.save(u);
		
		User uLoaded = ds.find(User.class).get();
		Assert.assertEquals("never, never", uLoaded.ignored);
	}
}
