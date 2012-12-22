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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.google.code.morphia.annotations.CappedAt;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Index;
import com.google.code.morphia.annotations.Indexed;
import com.google.code.morphia.annotations.Indexes;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.utils.IndexDirection;
import com.google.code.morphia.utils.IndexFieldDef;
import com.mongodb.DBObject;

/**
 * @author Scott Hernandez
 */
@SuppressWarnings("unused")
public class TestIndexedCapped extends TestBase {
	@Entity(cap = @CappedAt(count = 1))
	private static class CurrentStatus {
		@Id
		ObjectId id;
		String message;
		
		private CurrentStatus() {
		}
		
		public CurrentStatus(String msg) {
			message = msg;
		}
	}
	
	private static class IndexedClass {
		@Id
		ObjectId id;
		@Indexed
		long l = 4;
	}
	
	@Entity
	private static class NamedIndexClass {
		@Id
		ObjectId id;
		@Indexed(name = "l_ascending")
		long l = 4;
	}
	
	@Entity
	private static class UniqueIndexClass {
		@Id
		ObjectId id;
		@Indexed(name = "l_ascending", unique = true)
		long l = 4;
		String name;
		
		UniqueIndexClass() {
		}
		
		UniqueIndexClass(String name) {
			this.name = name;
		}
	}
	
	private static class Ad {
		@Id
		public long id;
		
		@Property("lastMod")
		@Indexed
		public long lastModified;
		
		@Indexed
		public boolean active;
	}
	
	@Indexes(@Index("active,-lastModified"))
	private static class Ad2 {
		@Id
		public long id;
		
		@Property("lastMod")
		@Indexed
		public long lastModified;
		
		@Indexed
		public boolean active;
	}
	
	@Embedded
	private static class IndexedEmbed {
		@Indexed(IndexDirection.DESC)
		String name;
	}
	
	private static class ContainsIndexedEmbed {
		@Id
		ObjectId id;
		IndexedEmbed e;
	}
	
	private static class CircularEmbeddedEntity {
		@Id
		ObjectId id = new ObjectId();
		String name;
		@Indexed
		CircularEmbeddedEntity a;
	}
	
	@Before
	@Override
	public void setUp() {
		super.setUp();
		morphia.map(CurrentStatus.class).map(UniqueIndexClass.class).map(IndexedClass.class).map(NamedIndexClass.class);
	}
	
	@Test
	public void testCappedEntity() throws Exception {
		ds.ensureCaps();
		CurrentStatus cs = new CurrentStatus("All Good");
		ds.save(cs);
		assertEquals(ds.getCount(CurrentStatus.class), 1);
		ds.save(new CurrentStatus("Kinda Bad"));
		assertEquals(ds.getCount(CurrentStatus.class), 1);
		assertTrue(ds.find(CurrentStatus.class).limit(1).get().message.contains("Bad"));
		ds.save(new CurrentStatus("Kinda Bad2"));
		assertEquals(ds.getCount(CurrentStatus.class), 1);
		ds.save(new CurrentStatus("Kinda Bad3"));
		assertEquals(ds.getCount(CurrentStatus.class), 1);
		ds.save(new CurrentStatus("Kinda Bad4"));
		assertEquals(ds.getCount(CurrentStatus.class), 1);
	}
	
	@Test
	public void testIndexes() {
		MappedClass mc = this.morphia.getMapper().addMappedClass(Ad2.class);
		
		assertFalse(hasNamedIndex("active_1_lastMod_-1", db.getCollection(mc.getCollectionName()).getIndexInfo()));
		ds.ensureIndexes(Ad2.class);
		assertTrue(hasNamedIndex("active_1_lastMod_-1", db.getCollection(mc.getCollectionName()).getIndexInfo()));
	}
	
	@Test
	public void testEmbeddedIndex() {
		MappedClass mc = this.morphia.getMapper().addMappedClass(ContainsIndexedEmbed.class);
		
		assertFalse(hasNamedIndex("e.name_-1", db.getCollection(mc.getCollectionName()).getIndexInfo()));
		ds.ensureIndexes(ContainsIndexedEmbed.class);
		assertTrue(hasNamedIndex("e.name_-1", db.getCollection(mc.getCollectionName()).getIndexInfo()));
	}
	
	@Test
	public void testMultipleIndexedFields() {
		MappedClass mc = morphia.getMapper().getMappedClass(Ad.class);
		this.morphia.map(Ad.class);
		
		IndexFieldDef[] defs = { new IndexFieldDef("lastMod"), new IndexFieldDef("active", IndexDirection.DESC) };
		assertFalse(hasNamedIndex("lastMod_1_active_-1", db.getCollection(mc.getCollectionName()).getIndexInfo()));
		ds.ensureIndex(Ad.class, defs);
		assertTrue(hasNamedIndex("lastMod_1_active_-1", db.getCollection(mc.getCollectionName()).getIndexInfo()));
	}
	
	@Test
	public void testIndexedRecursiveEntity() throws Exception {
		MappedClass mc = morphia.getMapper().getMappedClass(CircularEmbeddedEntity.class);
		ds.ensureIndexes();
		assertTrue(hasNamedIndex("a_1", db.getCollection(mc.getCollectionName()).getIndexInfo()));
	}
	
	@Test
	public void testIndexedEntity() throws Exception {
		MappedClass mc = morphia.getMapper().getMappedClass(IndexedClass.class);
		ds.ensureIndexes();
		assertTrue(hasIndexedField("l", db.getCollection(mc.getCollectionName()).getIndexInfo()));
		ds.save(new IndexedClass());
		ds.ensureIndexes();
		assertTrue(hasIndexedField("l", db.getCollection(mc.getCollectionName()).getIndexInfo()));
	}
	
	@Test
	public void testUniqueIndexedEntity() throws Exception {
		MappedClass mc = morphia.getMapper().getMappedClass(UniqueIndexClass.class);
		ds.ensureIndexes();
		assertTrue(hasIndexedField("l", db.getCollection(mc.getCollectionName()).getIndexInfo()));
		ds.save(new UniqueIndexClass("a"));
		
		try {
			// this should throw...
			ds.save(new UniqueIndexClass("v"));
			assertTrue(false);
			// } catch (MappingException me) {}
		} catch (Throwable me) {
		} // currently is masked by java.lang.RuntimeException: json can't
			// serialize type : class com.mongodb.DBTimestamp
		
		ds.ensureIndexes();
		assertTrue(hasIndexedField("l", db.getCollection(mc.getCollectionName()).getIndexInfo()));
	}
	
	@Test
	public void testNamedIndexEntity() throws Exception {
		MappedClass mc = morphia.getMapper().getMappedClass(NamedIndexClass.class);
		ds.ensureIndexes();
		assertTrue(hasIndexedField("l", db.getCollection(mc.getCollectionName()).getIndexInfo()));
		ds.save(new IndexedClass());
		ds.ensureIndexes();
		assertTrue(hasIndexedField("l", db.getCollection(mc.getCollectionName()).getIndexInfo()));
		
		assertTrue(hasNamedIndex("l_ascending", db.getCollection(mc.getCollectionName()).getIndexInfo()));
	}
	
	protected boolean hasNamedIndex(String name, List<DBObject> indexes) {
		for (DBObject dbObj : indexes) {
			if (dbObj.get("name").equals(name))
				return true;
		}
		return false;
	}
	
	protected boolean hasIndexedField(String name, List<DBObject> indexes) {
		for (DBObject dbObj : indexes) {
			if (((DBObject) dbObj.get("key")).containsField(name))
				return true;
		}
		return false;
	}
}
