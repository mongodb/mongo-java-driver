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

import static org.junit.Assert.assertEquals;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.annotations.Id;

/**
 *
 * @author Scott Hernandez
 */
public class TestDatastoreMerge  extends TestBase {

	private static class TestEntity {
		@Id ObjectId id;
		String name;
		String foo;
		int position = 0;
	}
	
	@Test
    public void testMerge() throws Exception {
		TestEntity te = new TestEntity();
		te.name = "test1";
		te.foo  = "bar";
		te.position = 1;
		ds.save(te);
		
		assertEquals(1, ds.getCount(te));
		
		//only update the position field with merge, normally save would override the whole object.
		TestEntity te2 = new TestEntity();
		te2.id = te.id;
		te2.position = 5;
		ds.merge(te2);

		TestEntity teLoaded = ds.get(te);
		assertEquals(teLoaded.name, te.name);
		assertEquals(teLoaded.foo, te.foo);
		assertEquals(teLoaded.position, te2.position);
	}
}
