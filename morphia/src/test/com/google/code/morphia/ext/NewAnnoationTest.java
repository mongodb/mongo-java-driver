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
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.code.morphia.EntityInterceptor;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;

/**
 *
 * @author Scott Hernandez
 */
public class NewAnnoationTest extends TestBase {

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.FIELD})
	static @interface Lowercase {
	}
	
	private static class User {
		@Id String id;
		@Lowercase
		String email;
	}

	private static class ToLowercaseHelper implements EntityInterceptor {
		public void postLoad(Object ent, DBObject dbObj, Mapper mapr) {
		}

		public void postPersist(Object ent, DBObject dbObj, Mapper mapr) {}

		public void preSave(Object ent, DBObject dbObj, Mapper mapr) {}

		public void preLoad(Object ent, DBObject dbObj, Mapper mapr) {}

		public void prePersist(Object ent, DBObject dbObj, Mapper mapr) {
			MappedClass mc = mapr.getMappedClass(ent);
			List<MappedField> toLowercase = mc.getFieldsAnnotatedWith(Lowercase.class);
			for (MappedField mf : toLowercase) {
				try {
					Object fieldValue = mf.getFieldValue(ent);
					dbObj.put(mf.getNameToStore() + "_lowercase", fieldValue.toString().toLowerCase());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	@Test
	public void testIt() {
		MappedField.interestingAnnotations.add(Lowercase.class);
		morphia.getMapper().addInterceptor(new ToLowercaseHelper());
		morphia.map(User.class);
		User u = new User();
		u.email = "ScottHernandez@gmail.com";
		
		ds.save(u);
		
		User uScott = ds.find(User.class).disableValidation().filter("email_lowercase", u.email.toLowerCase()).get();
		Assert.assertNotNull(uScott);
		
	}
}
