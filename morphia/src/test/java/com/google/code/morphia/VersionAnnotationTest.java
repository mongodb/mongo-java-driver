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

package com.google.code.morphia;

import java.util.ConcurrentModificationException;

import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Version;

/**
 *
 * @author Scott Hernandez
 */

public class VersionAnnotationTest extends TestBase {

	private static class B {
		@Id ObjectId id = new ObjectId();
		@Version long version;
	}

	@Ignore @Test(expected=ConcurrentModificationException.class)
	public void testVersion() throws Exception {

		B b1 = new B();
		try {ds.save(b1); } catch (Exception e) {throw new RuntimeException(e);}
		B b2 = new B();
		b2.id = b1.id;
		ds.save(b2);
	}


}