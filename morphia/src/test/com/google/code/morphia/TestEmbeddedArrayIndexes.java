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
import static org.junit.Assert.assertNotNull;

import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Index;
import com.google.code.morphia.annotations.Indexed;
import com.google.code.morphia.annotations.Indexes;
import com.google.code.morphia.mapping.MappedClass;
import com.mongodb.DBCollection;

/**
 *
 * @author Scott Hernandez
 */
public class TestEmbeddedArrayIndexes extends TestBase {
	@Indexes({ @Index("b.bar, b.car") })
	private static class A {
		@Id ObjectId id = new ObjectId();
		Set<B> b;
		@Indexed String foo;
	}

	private static class B {
		String bar;
		String car;
	}

    @Test
    public void testParamEntity() throws Exception {
    	MappedClass mc = morphia.getMapper().getMappedClass(A.class);
    	assertNotNull(mc);
    	
    	assertEquals(1, mc.getAnnotations(Indexes.class).size());
    	
    	ds.ensureIndexes(A.class);
    	DBCollection coll = ds.getCollection(A.class);
    	
    	assertEquals("indexes found: coll.getIndexInfo()" + coll.getIndexInfo() , 3, coll.getIndexInfo().size());
    	
}
    
}
