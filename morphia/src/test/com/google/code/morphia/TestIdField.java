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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.testmodel.Rectangle;
import com.mongodb.BasicDBObject;


/**
 *
 * @author Scott Hernandez
 */
@SuppressWarnings({"unused"})
public class TestIdField extends TestBase {

	@Entity
	private static class ReferenceAsId {
		@Id @Reference Rectangle id;
		
		protected ReferenceAsId() {}
		public ReferenceAsId(Rectangle key) {
			this.id = key;
		}
	}

	@Entity
	private static class KeyAsId {
		@Id Key<?> id;
		
		protected KeyAsId() {}
		public KeyAsId(Key<?> key) {
			this.id = key;
		}
	}

	@Entity
	private static class MapAsId {
		@Id Map<String, String> id = new HashMap<String, String>();
	}

	@Test @Ignore("need to set the _db in the dbref for this to work... see issue 90, ")
    public void testReferenceAsId() throws Exception {
        morphia.map(ReferenceAsId.class);
        
        Rectangle r = new Rectangle(1,1);
        Key<Rectangle> rKey = ds.save(r);

        ReferenceAsId rai = new ReferenceAsId(r);
        Key<ReferenceAsId> raiKey = ds.save(rai);
        ReferenceAsId raiLoaded = ds.get(ReferenceAsId.class, rKey);
        assertNotNull(raiLoaded);
        assertEquals(raiLoaded.id.getArea(), r.getArea(), 0);
        
        assertNotNull(raiKey);
	}

	@Test
    public void testKeyAsId() throws Exception {
        morphia.map(KeyAsId.class);
        
        Rectangle r = new Rectangle(1,1);
//        Rectangle r2 = new Rectangle(11,11);
        
        Key<Rectangle> rKey = ds.save(r);
//        Key<Rectangle> r2Key = ds.save(r2);
        KeyAsId kai = new KeyAsId(rKey);
        Key<KeyAsId> kaiKey = ds.save(kai);
        KeyAsId kaiLoaded = ds.get(KeyAsId.class, rKey);
        assertNotNull(kaiLoaded);
        assertNotNull(kaiKey);
	}

	@Test
    public void testMapAsId() throws Exception {
        morphia.map(MapAsId.class);
        
        MapAsId mai = new MapAsId();
        mai.id.put("test", "string");
        Key<MapAsId> maiKey = ds.save(mai);
        MapAsId maiLoaded = ds.get(MapAsId.class, new BasicDBObject("test","string"));
        assertNotNull(maiLoaded);
        assertNotNull(maiKey);
	}

	@Test
    public void testIdFieldNameMapping() throws Exception {
		Rectangle r = new Rectangle(1, 12);
		BasicDBObject dbObj = (BasicDBObject) morphia.toDBObject(r);
        assertFalse(dbObj.containsField("id"));
        assertTrue(dbObj.containsField(Mapper.ID_KEY));
        assertEquals(4, dbObj.size()); //_id, h, w, className
	}
}