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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import com.google.code.morphia.TestInheritanceMappings.MapLike;
import com.google.code.morphia.annotations.AlsoLoad;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.mapping.cache.DefaultEntityCache;
import com.google.code.morphia.testmodel.Address;
import com.google.code.morphia.testmodel.Article;
import com.google.code.morphia.testmodel.Circle;
import com.google.code.morphia.testmodel.Hotel;
import com.google.code.morphia.testmodel.PhoneNumber;
import com.google.code.morphia.testmodel.Rectangle;
import com.google.code.morphia.testmodel.RecursiveChild;
import com.google.code.morphia.testmodel.RecursiveParent;
import com.google.code.morphia.testmodel.Translation;
import com.google.code.morphia.testmodel.TravelAgency;
import com.google.code.morphia.testutil.AssertedFailure;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

/**
 * @author Olafur Gauti Gudmundsson
 * @author Scott Hernandez
 */
@SuppressWarnings({"unchecked", "rawtypes", "unused"})
public class TestMapping  extends TestBase {

	public static abstract class BaseEntity implements Serializable{
		private static final long serialVersionUID = 1L;

		public BaseEntity() {}
	
		// generally a bad thing but left over...
		@Id ObjectId id;
		public String getId() {
			return id.toString();
		}
	
       public void setId(String id) {
    	   this.id = new ObjectId(id);
       }
	}

	
	@Entity
	private static class MissingId {
		String id;
	}
	
	private static class MissingIdStill {
		String id;
	}
	
	@Entity("no-id")
	private static class MissingIdRenamed {
		String id;
	}

	@Embedded
	private static class IdOnEmbedded {
		@Id ObjectId id;
	}
	
	@Embedded("no-id")
	private static class RenamedEmbedded {
		String name;
	}

	private static class StranglyNamedIdField {
		@Id ObjectId id_ = new ObjectId();
	}
	
	private static class ContainsEmbeddedArray {
		@Id ObjectId id = new ObjectId();
		RenamedEmbedded[] res;
	}
	
	private static class NotEmbeddable {
		String noImNot = "no, I'm not";
	}
	private static class SerializableClass implements Serializable {
		private static final long serialVersionUID = 1L;
		String someString = "hi, from the ether.";
	}

	private static class ContainsRef {
		public @Id ObjectId id;
		public DBRef rect;
	}

	private static class HasFinalFieldId{
		public final @Id long id;
		public String name = "some string";
		
		//only called when loaded by the persistence framework.
		protected HasFinalFieldId() {
			id = -1;
		}
		
		public HasFinalFieldId(long id) {
			this.id = id;
		}
	}

	private static class ContainsFinalField{
		public @Id ObjectId id;
		public final String name;
		
		protected ContainsFinalField() {
			name = "foo";
		}
		
		public ContainsFinalField(String name) {
			this.name = name;
		}
	}

	private static class ContainsTimestamp {
		@Id ObjectId id;
		Timestamp ts = new Timestamp(System.currentTimeMillis());
	}

	private static class ContainsDBObject {
		@Id ObjectId id;
		DBObject dbObj = BasicDBObjectBuilder.start("field", "val").get();
	}
	
	private static class ContainsbyteArray {
		@Id ObjectId id;
		byte[] bytes = "Scott".getBytes();
	}

	private static class ContainsSerializedData{
		@Id ObjectId id;
		@Serialized SerializableClass data = new SerializableClass();
	}

	private static class ContainsLongAndStringArray {
		@Id ObjectId id;
		private Long[] longs = {0L, 1L, 2L};
		String[] strings = {"Scott", "Rocks"};
	}
	
	private static class ContainsCollection {
		@Id ObjectId id;
		Collection<String> coll = new ArrayList<String>();
		
		private ContainsCollection() {
			coll.add("hi"); coll.add("Scott");
		}
	}
	
	private static class ContainsPrimitiveMap{
		@Id ObjectId id;
		@Embedded public Map<String, Long> embeddedValues = new HashMap();
		public Map<String, Long> values = new HashMap();
	}

	private static interface Foo {
	}
	
	private static class Foo1 implements Foo {
		String s;
	}

	private static class Foo2 implements Foo {
		int i;
	}
	
	private static class ContainsMapWithEmbeddedInterface{
		@Id ObjectId id;
		@Embedded public Map<String, Foo> embeddedValues = new HashMap();
	}

	private static class ContainsEmbeddedEntity{
		@Id ObjectId id = new ObjectId();
		@Embedded ContainsIntegerList cil = new ContainsIntegerList();
	}

	public enum Enum1 { A, B }

	@Entity(value="cil", noClassnameStored=true)
 	private static class ContainsIntegerList {
		@Id ObjectId id;
		List<Integer> intList = new ArrayList<Integer>();
	}

 	private static class ContainsIntegerListNewAndOld {
		@Id ObjectId id;
		List<Integer> intList = new ArrayList<Integer>();
		List<Integer> ints = new ArrayList<Integer>();
	}

	@Entity(value="cil", noClassnameStored=true)
	private static class ContainsIntegerListNew {
		@Id ObjectId id;
		@AlsoLoad("intList") List<Integer> ints = new ArrayList<Integer>();
	}
	
	@Entity(noClassnameStored=true)
	private static class ContainsUUID {
		@Id ObjectId id;
		UUID uuid = UUID.randomUUID();
	}

	@Entity(noClassnameStored=true)
	private static class ContainsUuidId {
		@Id UUID id = UUID.randomUUID();
	}
	
	private static class ContainsEnum1KeyMap{
		@Id ObjectId id;
		public Map<Enum1, String> values = new HashMap<Enum1,String>();
		@Embedded
		public Map<Enum1, String> embeddedValues = new HashMap<Enum1,String>();
	}

	private static class ContainsIntKeyMap {
		@Id ObjectId id;
		public Map<Integer, String> values = new HashMap<Integer,String>();
	}

	private static class ContainsIntKeySetStringMap {
		@Id ObjectId id;
		@Embedded
		public Map<Integer, Set<String>> values = new HashMap<Integer,Set<String>>();
	}
	
	private static class ContainsObjectIdKeyMap{
		@Id ObjectId id;
		public Map<ObjectId, String> values = new HashMap<ObjectId,String>();
	}
	
	private static class ContainsXKeyMap<T>{
		@Id ObjectId id;
		public Map<T, String> values = new HashMap<T,String>();
	}
	
	private static class ContainsMapLike {
		@Id ObjectId id;
		MapLike m = new MapLike();
	}
	
	@Entity
	private static class UsesBaseEntity extends BaseEntity{
		private static final long serialVersionUID = 1L;
		
	}

	private static class MapSubclass extends LinkedHashMap<String, Object> {
		private static final long serialVersionUID = 1L;
		@Id ObjectId id;
	}
	
	private class NonStaticInnerClass {
		@Id long id = 1;
	}

	@Test
    public void testUUID() throws Exception {
		morphia.map(ContainsUUID.class);
		ContainsUUID cuuid = new ContainsUUID();
		UUID before = cuuid.uuid;
		ds.save(cuuid);
		ContainsUUID loaded = ds.find(ContainsUUID.class).get();
		assertNotNull(loaded);
		assertNotNull(loaded.id);
		assertNotNull(loaded.uuid);
		assertEquals(before, loaded.uuid);
	}
	@Test
    public void testEmbeddedDBObject() throws Exception {
		morphia.map(ContainsDBObject.class);
		ContainsDBObject cdbo = new ContainsDBObject();
		ds.save(cdbo);
		assertNotNull(ds.find(ContainsDBObject.class).get());
	}
	
	@Test
    public void testUuidId() throws Exception {
		morphia.map(ContainsUuidId.class);
		ContainsUuidId cuuidId = new ContainsUuidId();
		UUID before = cuuidId.id;
		Key<ContainsUuidId> key = ds.save(cuuidId);
		ContainsUuidId loaded = ds.get(ContainsUuidId.class, before);
		assertNotNull(loaded);
		assertNotNull(loaded.id);
		assertEquals(before, loaded.id);
	}

	@Test
    public void testEmbeddedEntity() throws Exception {
		morphia.map(ContainsEmbeddedEntity.class);
		ContainsEmbeddedEntity cee = new ContainsEmbeddedEntity();
		ds.save(cee);
		ContainsEmbeddedEntity ceeLoaded = ds.find(ContainsEmbeddedEntity.class).get();
		assertNotNull(ceeLoaded);
		assertNotNull(ceeLoaded.id);
		assertNotNull(ceeLoaded.cil);
		assertNull(ceeLoaded.cil.id);
		
		
	}
	
	@Test
    public void testEmbeddedArrayElementHasNoClassname() throws Exception {
		morphia.map(ContainsEmbeddedArray.class);
		ContainsEmbeddedArray cea = new ContainsEmbeddedArray();
		cea.res = new RenamedEmbedded[] { new RenamedEmbedded() };
		
		DBObject dbObj = morphia.toDBObject(cea);
		assertTrue(!((DBObject)((List)dbObj.get("res")).get(0)).containsField(Mapper.CLASS_NAME_FIELDNAME));
	}

	@Test
    public void testEmbeddedEntityDBObjectHasNoClassname() throws Exception {
		morphia.map(ContainsEmbeddedEntity.class);
		ContainsEmbeddedEntity cee = new ContainsEmbeddedEntity();
		cee.cil = new ContainsIntegerList();
		cee.cil.intList = Collections.singletonList(1);
		DBObject dbObj = morphia.toDBObject(cee);
		assertTrue(!((DBObject)dbObj.get("cil")).containsField(Mapper.CLASS_NAME_FIELDNAME));
	}

	@Test
    public void testEnumKeyedMap() throws Exception {
		ContainsEnum1KeyMap map = new ContainsEnum1KeyMap();
		map.values.put(Enum1.A,"I'm a");
		map.values.put(Enum1.B,"I'm b");
		map.embeddedValues.put(Enum1.A,"I'm a");
		map.embeddedValues.put(Enum1.B,"I'm b");
		
		Key<?> mapKey = ds.save(map);
		
		ContainsEnum1KeyMap mapLoaded = ds.get(ContainsEnum1KeyMap.class, mapKey.getId());
		
		assertNotNull(mapLoaded);
		assertEquals(2,mapLoaded.values.size());
		assertNotNull(mapLoaded.values.get(Enum1.A));
		assertNotNull(mapLoaded.values.get(Enum1.B));
		assertEquals(2,mapLoaded.embeddedValues.size());
		assertNotNull(mapLoaded.embeddedValues.get(Enum1.A));
		assertNotNull(mapLoaded.embeddedValues.get(Enum1.B));
	}

	@Test
    public void testAlsoLoad() throws Exception {
		ContainsIntegerList cil = new ContainsIntegerList();
		cil.intList.add(1);
		ds.save(cil);
		ContainsIntegerList cilLoaded = ds.get(cil);
		assertNotNull(cilLoaded);
		assertNotNull(cilLoaded.intList);
		assertEquals(cilLoaded.intList.size(), cil.intList.size());
		assertEquals(cilLoaded.intList.get(0), cil.intList.get(0));
		
		ContainsIntegerListNew cilNew = ds.get(ContainsIntegerListNew.class, cil.id);
		assertNotNull(cilNew);
		assertNotNull(cilNew.ints);
		assertEquals(cilNew.ints.size(), 1);
		assertEquals(1, (int)cil.intList.get(0));
	}

	@Test
    public void testIntLists() throws Exception {
		ContainsIntegerList cil = new ContainsIntegerList();
		ds.save(cil);
		ContainsIntegerList cilLoaded = ds.get(cil);
		assertNotNull(cilLoaded);
		assertNotNull(cilLoaded.intList);
		assertEquals(cilLoaded.intList.size(), cil.intList.size());

		
		cil = new ContainsIntegerList();
		cil.intList = null;
		ds.save(cil);
		cilLoaded = ds.get(cil);
		assertNotNull(cilLoaded);
		assertNotNull(cilLoaded.intList);
		assertEquals(cilLoaded.intList.size(), 0);

		cil = new ContainsIntegerList();
		cil.intList.add(1);
		ds.save(cil);
		cilLoaded = ds.get(cil);
		assertNotNull(cilLoaded);
		assertNotNull(cilLoaded.intList);
		assertEquals(cilLoaded.intList.size(), 1);
		assertEquals(1,(int)cilLoaded.intList.get(0));
	}
    
    @Test
    public void testObjectIdKeyedMap() throws Exception {
    	morphia.map(ContainsObjectIdKeyMap.class);
    	ContainsObjectIdKeyMap map = new ContainsObjectIdKeyMap();
    	ObjectId o1 = new ObjectId("111111111111111111111111");
    	ObjectId o2 = new ObjectId("222222222222222222222222");
		map.values.put(o1,"I'm 1s");
		map.values.put(o2,"I'm 2s");
		
		Key<?> mapKey = ds.save(map);
		
		ContainsObjectIdKeyMap mapLoaded = ds.get(ContainsObjectIdKeyMap.class, mapKey.getId());
		
		assertNotNull(mapLoaded);
		assertEquals(2,mapLoaded.values.size());
		assertNotNull(mapLoaded.values.get(o1));
		assertNotNull(mapLoaded.values.get(o2));
		
		assertNotNull(ds.find(ContainsIntKeyMap.class).field("values.111111111111111111111111").exists());
		assertEquals(0, ds.find(ContainsIntKeyMap.class).field("values.111111111111111111111111").doesNotExist().countAll());
		assertNotNull(ds.find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
		assertEquals(0, ds.find(ContainsIntKeyMap.class).field("values.4").exists().countAll());
    }

	@Test
    public void testIntKeyedMap() throws Exception {
		ContainsIntKeyMap map = new ContainsIntKeyMap ();
		map.values.put(1,"I'm 1");
		map.values.put(2,"I'm 2");
		
		Key<?> mapKey = ds.save(map);
		
		ContainsIntKeyMap  mapLoaded = ds.get(ContainsIntKeyMap.class, mapKey.getId());
		
		assertNotNull(mapLoaded);
		assertEquals(2,mapLoaded.values.size());
		assertNotNull(mapLoaded.values.get(1));
		assertNotNull(mapLoaded.values.get(2));
		
		assertNotNull(ds.find(ContainsIntKeyMap.class).field("values.2").exists());
		assertEquals(0, ds.find(ContainsIntKeyMap.class).field("values.2").doesNotExist().countAll());
		assertNotNull(ds.find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
		assertEquals(0, ds.find(ContainsIntKeyMap.class).field("values.4").exists().countAll());
	}

	@Test
    public void testIntKeySetStringMap() throws Exception {
		ContainsIntKeySetStringMap map = new ContainsIntKeySetStringMap();
		map.values.put(1, Collections.singleton("I'm 1"));
		map.values.put(2, Collections.singleton("I'm 2"));
		
		Key<?> mapKey = ds.save(map);
		
		ContainsIntKeySetStringMap  mapLoaded = ds.get(ContainsIntKeySetStringMap.class, mapKey.getId());
		
		assertNotNull(mapLoaded);
		assertEquals(2,mapLoaded.values.size());
		assertNotNull(mapLoaded.values.get(1));
		assertNotNull(mapLoaded.values.get(2));
		assertEquals(1,mapLoaded.values.get(1).size());
		
		assertNotNull(ds.find(ContainsIntKeyMap.class).field("values.2").exists());
		assertEquals(0, ds.find(ContainsIntKeyMap.class).field("values.2").doesNotExist().countAll());
		assertNotNull(ds.find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
		assertEquals(0, ds.find(ContainsIntKeyMap.class).field("values.4").exists().countAll());
	}

	@Test @Ignore("need to add this feature")
    public void testGenericKeyedMap() throws Exception {
		ContainsXKeyMap<Integer> map = new ContainsXKeyMap<Integer>();
		map.values.put(1,"I'm 1");
		map.values.put(2,"I'm 2");
		
		Key<?> mapKey = ds.save(map);
		
		ContainsXKeyMap<Integer> mapLoaded = ds.get(ContainsXKeyMap.class, mapKey.getId());
		
		assertNotNull(mapLoaded);
		assertEquals(2,mapLoaded.values.size());
		assertNotNull(mapLoaded.values.get(1));
		assertNotNull(mapLoaded.values.get(2));
	}

	@Test
    public void testMapLike() throws Exception {
		ContainsMapLike ml = new ContainsMapLike();
		ml.m.put("first","test");
		ds.save(ml);
		ContainsMapLike mlLoaded = ds.find(ContainsMapLike.class).get();
		assertNotNull(mlLoaded);
		assertNotNull(mlLoaded.m);
		assertNotNull(mlLoaded.m.containsKey("first"));
	}
	
	@Test
    public void testPrimMap() throws Exception {
		ContainsPrimitiveMap primMap = new ContainsPrimitiveMap();
		primMap.embeddedValues.put("first",1L);
		primMap.embeddedValues.put("second",2L);
		primMap.values.put("first",1L);
		primMap.values.put("second",2L);
		Key<ContainsPrimitiveMap> primMapKey = ds.save(primMap);
		
		ContainsPrimitiveMap primMapLoaded = ds.get(ContainsPrimitiveMap.class, primMapKey.getId());
		
		assertNotNull(primMapLoaded);
		assertEquals(2,primMapLoaded.embeddedValues.size());
		assertEquals(2,primMapLoaded.values.size());
	}

	@Test
    public void testPrimMapWithNullValue() throws Exception {
		ContainsPrimitiveMap primMap = new ContainsPrimitiveMap();
		primMap.embeddedValues.put("first",null);
		primMap.embeddedValues.put("second",2L);
		primMap.values.put("first",null);
		primMap.values.put("second",2L);
		Key<ContainsPrimitiveMap> primMapKey = ds.save(primMap);
		
		ContainsPrimitiveMap primMapLoaded = ds.get(ContainsPrimitiveMap.class, primMapKey.getId());
		
		assertNotNull(primMapLoaded);
		assertEquals(2,primMapLoaded.embeddedValues.size());
		assertEquals(2,primMapLoaded.values.size());
	}

	@Test
    public void testMapWithEmbeddedInterface() throws Exception {
		ContainsMapWithEmbeddedInterface aMap = new ContainsMapWithEmbeddedInterface();
		Foo f1 = new Foo1();
		Foo f2 = new Foo2();
		
		aMap.embeddedValues.put("first",f1);
		aMap.embeddedValues.put("second",f2);
		ds.save(aMap);
		
		ContainsMapWithEmbeddedInterface mapLoaded = ds.find(ContainsMapWithEmbeddedInterface.class).get();
		
		assertNotNull(mapLoaded);
		assertEquals(2, mapLoaded.embeddedValues.size());
		assertTrue(mapLoaded.embeddedValues.get("first") instanceof Foo1);
		assertTrue(mapLoaded.embeddedValues.get("second") instanceof Foo2);
		
	}
	
	@Test
    public void testIdFieldWithUnderscore() throws Exception {
		morphia.map(StranglyNamedIdField.class);
	}
	
	@Test
    public void testFinalIdField() throws Exception {
		morphia.map(HasFinalFieldId.class);
		Key<HasFinalFieldId> savedKey = ds.save(new HasFinalFieldId(12));
		HasFinalFieldId loaded = ds.get(HasFinalFieldId.class, savedKey.getId());
		assertNotNull(loaded);        
		assertNotNull(loaded.id);        
		assertEquals(loaded.id, 12);
	}

	@Test
    public void testFinalField() throws Exception {
		morphia.map(ContainsFinalField.class);
		Key<ContainsFinalField> savedKey = ds.save(new ContainsFinalField("blah"));
		ContainsFinalField loaded = ds.get(ContainsFinalField.class, savedKey.getId());
		assertNotNull(loaded);        
		assertNotNull(loaded.name);        
		assertEquals("blah",loaded.name);
	}

	@Test
    public void testFinalFieldNotPersisted() throws Exception {
		((DatastoreImpl)ds).getMapper().getOptions().ignoreFinals = true;
		morphia.map(ContainsFinalField.class);
		Key<ContainsFinalField> savedKey = ds.save(new ContainsFinalField("blah"));
		ContainsFinalField loaded = ds.get(ContainsFinalField.class, savedKey.getId());
		assertNotNull(loaded);        
		assertNotNull(loaded.name);        
		assertEquals("foo", loaded.name);
	}

	@Test
    public void testTimestampMapping() throws Exception {
		morphia.map(ContainsTimestamp.class);
		ContainsTimestamp cts = new ContainsTimestamp();
		Key<ContainsTimestamp> savedKey = ds.save(cts);
		ContainsTimestamp loaded = ds.get(ContainsTimestamp.class, savedKey.getId());
		assertNotNull(loaded.ts);
		assertEquals(loaded.ts.getTime(), cts.ts.getTime());

	}

	@Test
    public void testCollectionMapping() throws Exception {
		morphia.map(ContainsCollection.class);
		Key<ContainsCollection> savedKey = ds.save(new ContainsCollection());
		ContainsCollection loaded = ds.get(ContainsCollection.class, savedKey.getId());
		assertEquals(loaded.coll, (new ContainsCollection()).coll);
		assertNotNull(loaded.id);        
	}
	
	@Test
    public void testbyteArrayMapping() throws Exception {
		morphia.map(ContainsbyteArray.class);
		Key<ContainsbyteArray> savedKey = ds.save(new ContainsbyteArray());
		ContainsbyteArray loaded = ds.get(ContainsbyteArray.class, savedKey.getId());
		assertEquals(new String((new ContainsbyteArray()).bytes), new String(loaded.bytes));
		assertNotNull(loaded.id);        
	}
	@Test
    public void testBaseEntityValidity() throws Exception {
		morphia.map(UsesBaseEntity.class);
	}	
	@Test
    public void testSerializedMapping() throws Exception {
		morphia.map(ContainsSerializedData.class);
		Key<ContainsSerializedData> savedKey = ds.save(new ContainsSerializedData());
		ContainsSerializedData loaded = ds.get(ContainsSerializedData.class, savedKey.getId());
		assertNotNull(loaded.data);        		
		assertEquals(loaded.data.someString, (new ContainsSerializedData()).data.someString);
		assertNotNull(loaded.id);        
	}

	@SuppressWarnings("deprecation")
	@Test
    public void testLongArrayMapping() throws Exception {
		morphia.map(ContainsLongAndStringArray.class);
		ds.save(new ContainsLongAndStringArray());
		ContainsLongAndStringArray loaded = ds.<ContainsLongAndStringArray>find(ContainsLongAndStringArray.class).get();
		assertEquals(loaded.longs, (new ContainsLongAndStringArray()).longs);
		assertEquals(loaded.strings, (new ContainsLongAndStringArray()).strings);
		
		ContainsLongAndStringArray clasa = new ContainsLongAndStringArray();
		clasa.strings = new String[] {"a", "B","c"};
		clasa.longs = new Long[] {4L, 5L, 4L};
		Key<ContainsLongAndStringArray> k1 = ds.save(clasa);
		loaded = ds.getByKey(ContainsLongAndStringArray.class, k1);
		assertEquals(loaded.longs, clasa.longs);
		assertEquals(loaded.strings, clasa.strings);
		
		assertNotNull(loaded.id);        
	}

	@Test
    public void testDbRefMapping() throws Exception {
        morphia.map(ContainsRef.class).map(Rectangle.class);
        DBCollection stuff = db.getCollection("stuff");
        DBCollection rectangles = db.getCollection("rectangles");
        
        assertTrue("'ne' field should not be persisted!", !morphia.getMapper().getMCMap().get(ContainsRef.class.getName()).containsJavaFieldName("ne"));

        Rectangle r = new Rectangle(1,1);
        DBObject rDbObject = morphia.toDBObject(r);
        rDbObject.put("_ns", rectangles.getName());
        rectangles.save(rDbObject);
        
        ContainsRef cRef = new ContainsRef();
        cRef.rect = new DBRef(null, (String)rDbObject.get("_ns"), rDbObject.get("_id"));
        DBObject cRefDbOject = morphia.toDBObject(cRef);
        stuff.save(cRefDbOject);
        BasicDBObject cRefDbObjectLoaded =(BasicDBObject)stuff.findOne(BasicDBObjectBuilder.start("_id", cRefDbOject.get("_id")).get());
		ContainsRef cRefLoaded = morphia.fromDBObject(ContainsRef.class, cRefDbObjectLoaded, new DefaultEntityCache());
        assertNotNull(cRefLoaded);
        assertNotNull(cRefLoaded.rect);
        assertNotNull(cRefLoaded.rect.getId());
        assertNotNull(cRefLoaded.rect.getRef());
        assertEquals(cRefLoaded.rect.getId(), cRef.rect.getId());
        assertEquals(cRefLoaded.rect.getRef(), cRef.rect.getRef());    
	}
	
	@Test
    public void testBadMappings() throws Exception {
        boolean allGood=false;
        try {
        	morphia.map(MissingId.class);
        } catch (MappingException e) {
        	allGood = true;
        }
        assertTrue("Validation: Missing @Id field not caught", allGood);

        allGood = false;
        try {
        	morphia.map(IdOnEmbedded.class);
        } catch (MappingException e) {
        	allGood = true;
        }
        assertTrue("Validation: @Id field on @Embedded not caught", allGood);

        allGood = false;
        try {
        	morphia.map(RenamedEmbedded.class);
        } catch (MappingException e) {
        	allGood = true;
        }
        assertTrue("Validation: @Embedded(\"name\") not caught on Class", allGood);

        allGood = false;
        try {
        	morphia.map(MissingIdStill.class);
        } catch (MappingException e) {
        	allGood = true;
        }
        assertTrue("Validation: Missing @Id field not not caught", allGood);

        allGood = false;
        try {
        	morphia.map(MissingIdRenamed.class);
        } catch (MappingException e) {
        	allGood = true;
        }
        assertTrue("Validation: Missing @Id field not not caught", allGood);
        
        allGood = false;
        try {
        	morphia.map(NonStaticInnerClass.class);
        } catch (MappingException e) {
        	allGood = true;
        }
        assertTrue("Validation: Non-static inner class allowed", allGood);    }
    
    
    @Test
    public void testBasicMapping() throws Exception {
        DBCollection hotels = db.getCollection("hotels");
        DBCollection agencies = db.getCollection("agencies");

        morphia.map(Hotel.class);
        morphia.map(TravelAgency.class);

        Hotel borg = Hotel.create();
        borg.setName("Hotel Borg");
        borg.setStars(4);
        borg.setTakesCreditCards(true);
        borg.setStartDate(new Date());
        borg.setType(Hotel.Type.LEISURE);
        borg.getTags().add("Swimming pool");
        borg.getTags().add("Room service");
        borg.setTemp("A temporary transient value");
        borg.getPhoneNumbers().add(new PhoneNumber(354,5152233,PhoneNumber.Type.PHONE));
        borg.getPhoneNumbers().add(new PhoneNumber(354,5152244,PhoneNumber.Type.FAX));

        Address borgAddr = new Address();
        borgAddr.setStreet("Posthusstraeti 11");
        borgAddr.setPostCode("101");
        borg.setAddress(borgAddr);
        
        BasicDBObject hotelDbObj = (BasicDBObject) morphia.toDBObject(borg);
        assertTrue( !( ((DBObject)((List)hotelDbObj.get("phoneNumbers")).get(0)).containsField(Mapper.CLASS_NAME_FIELDNAME)) ); 
        
        
        hotels.save(hotelDbObj);

		Hotel borgLoaded = morphia.fromDBObject(Hotel.class, hotelDbObj, new DefaultEntityCache());

        assertEquals(borg.getName(), borgLoaded.getName());
        assertEquals(borg.getStars(), borgLoaded.getStars());
        assertEquals(borg.getStartDate(), borgLoaded.getStartDate());
        assertEquals(borg.getType(), borgLoaded.getType());
        assertEquals(borg.getAddress().getStreet(), borgLoaded.getAddress().getStreet());
        assertEquals(borg.getTags().size(), borgLoaded.getTags().size());
        assertEquals(borg.getTags(), borgLoaded.getTags());
        assertEquals(borg.getPhoneNumbers().size(), borgLoaded.getPhoneNumbers().size());
        assertEquals(borg.getPhoneNumbers().get(1), borgLoaded.getPhoneNumbers().get(1));
        assertNull(borgLoaded.getTemp());
        assertTrue(borgLoaded.getPhoneNumbers() instanceof Vector);
        assertNotNull(borgLoaded.getId());

        TravelAgency agency = new TravelAgency();
        agency.setName("Lastminute.com");
        agency.getHotels().add(borgLoaded);
        
        BasicDBObject agencyDbObj = (BasicDBObject) morphia.toDBObject(agency);
        agencies.save(agencyDbObj);

		TravelAgency agencyLoaded = morphia.fromDBObject(TravelAgency.class,
				(BasicDBObject) agencies.findOne(new BasicDBObject(Mapper.ID_KEY, agencyDbObj.get(Mapper.ID_KEY))),
				new DefaultEntityCache());

        assertEquals(agency.getName(), agencyLoaded.getName());
        assertEquals(agency.getHotels().size(), 1);
        assertEquals(agency.getHotels().get(0).getName(), borg.getName());

        // try clearing values
        borgLoaded.setAddress(null);
        borgLoaded.getPhoneNumbers().clear();
        borgLoaded.setName(null);

        hotelDbObj = (BasicDBObject) morphia.toDBObject(borgLoaded);
        hotels.save(hotelDbObj);

        hotelDbObj = (BasicDBObject)hotels.findOne(new BasicDBObject(Mapper.ID_KEY, hotelDbObj.get(Mapper.ID_KEY)));

		borgLoaded = morphia.fromDBObject(Hotel.class, hotelDbObj, new DefaultEntityCache());
        assertNull(borgLoaded.getAddress());
        assertEquals(0, borgLoaded.getPhoneNumbers().size());
        assertNull(borgLoaded.getName());
    }
    
    @Test
    public void testMaps() throws Exception {
        DBCollection articles = db.getCollection("articles");
        morphia.map(Article.class).map(Translation.class).map(Circle.class);

        Article related = new Article();
        BasicDBObject relatedDbObj = (BasicDBObject) morphia.toDBObject(related);
        articles.save(relatedDbObj);

		Article relatedLoaded = morphia
				.fromDBObject(Article.class, (BasicDBObject) articles.findOne(new BasicDBObject(Mapper.ID_KEY,
						relatedDbObj.get(Mapper.ID_KEY))), new DefaultEntityCache());

        Article article = new Article();
        article.setTranslation("en", new Translation("Hello World", "Just a test"));
        article.setTranslation("is", new Translation("Halló heimur", "Bara að prófa"));

        article.setAttribute("myDate", new Date());
        article.setAttribute("myString", "Test");
        article.setAttribute("myInt", 123);

        article.putRelated("test", relatedLoaded);

        BasicDBObject articleDbObj = (BasicDBObject) morphia.toDBObject(article);
        articles.save(articleDbObj);

		Article articleLoaded = morphia
				.fromDBObject(Article.class, (BasicDBObject) articles.findOne(new BasicDBObject(Mapper.ID_KEY,
						articleDbObj.get(Mapper.ID_KEY))), new DefaultEntityCache());

        assertEquals(article.getTranslations().size(), articleLoaded.getTranslations().size());
        assertEquals(article.getTranslation("en").getTitle(), articleLoaded.getTranslation("en").getTitle());
        assertEquals(article.getTranslation("is").getBody(), articleLoaded.getTranslation("is").getBody());
        assertEquals(article.getAttributes().size(), articleLoaded.getAttributes().size());
        assertEquals(article.getAttribute("myDate"), articleLoaded.getAttribute("myDate"));
        assertEquals(article.getAttribute("myString"), articleLoaded.getAttribute("myString"));
        assertEquals(article.getAttribute("myInt"), articleLoaded.getAttribute("myInt"));
        assertEquals(article.getRelated().size(), articleLoaded.getRelated().size());
        assertEquals(article.getRelated("test").getId(), articleLoaded.getRelated("test").getId());
    }

    
    @Test
    public void testReferenceWithoutIdValue() throws Exception {
		new AssertedFailure(MappingException.class) {
			public void thisMustFail() throws Throwable {
		        RecursiveParent parent = new RecursiveParent();
		        RecursiveChild child = new RecursiveChild();
		        child.setId(null);
		        parent.setChild(child);
		        ds.save(parent);
			}
		};

    }

    @Test
    public void testRecursiveReference() throws Exception {
        DBCollection stuff = db.getCollection("stuff");

        morphia.map(RecursiveParent.class).map(RecursiveChild.class);

        RecursiveParent parent = new RecursiveParent();
        BasicDBObject parentDbObj = (BasicDBObject) morphia.toDBObject(parent);
        stuff.save(parentDbObj);

        RecursiveChild child = new RecursiveChild();
        BasicDBObject childDbObj = (BasicDBObject) morphia.toDBObject(child);
        stuff.save(childDbObj);

		RecursiveParent parentLoaded = morphia.fromDBObject(RecursiveParent.class,
				(BasicDBObject) stuff.findOne(new BasicDBObject(Mapper.ID_KEY, parentDbObj.get(Mapper.ID_KEY))),
				new DefaultEntityCache());
		RecursiveChild childLoaded = morphia.fromDBObject(RecursiveChild.class,
				(BasicDBObject) stuff.findOne(new BasicDBObject(Mapper.ID_KEY, childDbObj.get(Mapper.ID_KEY))),
				new DefaultEntityCache());

        parentLoaded.setChild(childLoaded);
        childLoaded.setParent(parentLoaded);

        stuff.save(morphia.toDBObject(parentLoaded));
        stuff.save(morphia.toDBObject(childLoaded));

		RecursiveParent finalParentLoaded = morphia.fromDBObject(RecursiveParent.class,
				(BasicDBObject) stuff.findOne(new BasicDBObject(Mapper.ID_KEY, parentDbObj.get(Mapper.ID_KEY))),
				new DefaultEntityCache());
		RecursiveChild finalChildLoaded = morphia.fromDBObject(RecursiveChild.class,
				(BasicDBObject) stuff.findOne(new BasicDBObject(Mapper.ID_KEY, childDbObj.get(Mapper.ID_KEY))),
				new DefaultEntityCache());

        assertNotNull(finalParentLoaded.getChild());
        assertNotNull(finalChildLoaded.getParent());
    }
}
