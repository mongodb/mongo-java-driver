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

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.NotSaved;

/**
 *
 * @author Scott Hernandez
 */
public class TestNotSaved  extends TestBase {

	@Entity(value="Normal", noClassnameStored=true)
	static class Normal {
		@Id ObjectId id = new ObjectId();
		String name;
		public Normal(String name) {this.name = name;}
		protected Normal() {}
	}

	@Entity(value="Normal", noClassnameStored=true)
	static class NormalWithNotSaved{
		@Id ObjectId id = new ObjectId();
		@NotSaved String name = "never";
	}

	@Test
    public void testBasic() throws Exception {
		ds.save(new Normal("value"));
		Normal n = ds.find(Normal.class).get();
		Assert.assertNotNull(n);
		Assert.assertNotNull(n.name);
		ds.delete(n);
		ds.save(new NormalWithNotSaved());
		n = ds.find(Normal.class).get();
		Assert.assertNotNull(n);
		Assert.assertNull(n.name);
		ds.delete(n);
		ds.save(new Normal("value21"));
		NormalWithNotSaved nwns = ds.find(NormalWithNotSaved.class).get();
		Assert.assertNotNull(nwns);
		Assert.assertNotNull(nwns.name);
		Assert.assertEquals("value21", nwns.name);
	}
}