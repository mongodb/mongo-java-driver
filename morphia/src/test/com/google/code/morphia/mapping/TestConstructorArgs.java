/**
 * Copyright (C) 2010 Scott Hernandez
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

package com.google.code.morphia.mapping;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.ConstructorArgs;
import com.google.code.morphia.annotations.Id;

/**
 *
 * @author Scott Hernandez
 */
public class TestConstructorArgs  extends TestBase {

	@SuppressWarnings("unused")
	private static class Normal {
		@Id ObjectId id = new ObjectId();
		@ConstructorArgs("_id")
		ArgsConstructor ac = new ArgsConstructor(new ObjectId());
	}

	private static class ArgsConstructor{
		final @Id ObjectId id;
		
		private ArgsConstructor(ObjectId id){
			this.id = id;
		}
	}

	@Test
    public void testBasic() throws Exception {
		Normal n = new Normal();
		ObjectId acId = n.ac.id;
		
		ds.save(n);
		n = ds.find(Normal.class).get();
		Assert.assertNotNull(n);
		Assert.assertNotNull(n.ac);
		Assert.assertEquals(acId, n.ac.id);
	}
}