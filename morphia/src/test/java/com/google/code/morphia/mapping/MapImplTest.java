/**
 * 
 */
package com.google.code.morphia.mapping;

import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import com.mongodb.BasicDBObject;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scott hernandez
 */
@SuppressWarnings("unused")
public class MapImplTest extends TestBase{
	
	private static class ContainsMapOfEmbeddedInterfaces{
		@Id ObjectId id;
		@Embedded Map<String, Serializable> values = new HashMap<String, Serializable>();
	}

	private static class ContainsMapOfEmbeddedGoos{
		@Id ObjectId id;
		@Embedded Map<String, Goo> values = new HashMap<String, Goo>();
	}
	
	private static class Goo implements Serializable {
		static final long serialVersionUID = 1L;
		@Id ObjectId id;
		String name;
		
		Goo(){}
		Goo(String n) {name=n;}
	}

	private static class E {
		@Id ObjectId id;

		@Embedded
		MyMap mymap = new MyMap();
	}
	
	private static class MyMap extends HashMap<String,String>{
		private static final long serialVersionUID = 1L;
	}
	
	@Test
	public void testMapping() throws Exception {
		E e = new E();
		e.mymap.put("1", "a");
		e.mymap.put("2", "b");
		
		ds.save(e);
		
		e = ds.get(e);
		Assert.assertEquals("a", e.mymap.get("1"));
		Assert.assertEquals("b", e.mymap.get("2"));
	}

    @Test
    public void testEmbeddedMap() throws Exception {
        morphia.map(ContainsMapOfEmbeddedGoos.class).map(ContainsMapOfEmbeddedInterfaces.class);
    	Goo g1 = new Goo("Scott");
    	ContainsMapOfEmbeddedGoos cmoeg = new ContainsMapOfEmbeddedGoos();
    	cmoeg.values.put("first", g1);
    	ds.save(cmoeg);
    	//check className in the map values.
    	
    	BasicDBObject goo = (BasicDBObject) ( (BasicDBObject) ds.getCollection(ContainsMapOfEmbeddedGoos.class).findOne().get("values") ).get("first");
    	boolean hasF = goo.containsField(Mapper.CLASS_NAME_FIELDNAME);
    	assertTrue(!hasF);
    }

    @Test
    public void testEmbeddedMapWithValueInterface() throws Exception {
        morphia.map(ContainsMapOfEmbeddedGoos.class).map(ContainsMapOfEmbeddedInterfaces.class);
    	Goo g1 = new Goo("Scott");
    	
    	ContainsMapOfEmbeddedInterfaces cmoei = new ContainsMapOfEmbeddedInterfaces();
    	cmoei.values.put("first", g1);
    	ds.save(cmoei);
    	//check className in the map values.
    	BasicDBObject goo = (BasicDBObject) ( (BasicDBObject) ds.getCollection(ContainsMapOfEmbeddedInterfaces.class).findOne().get("values") ).get("first");
    	boolean hasF = goo.containsField(Mapper.CLASS_NAME_FIELDNAME);
    	assertTrue(hasF);
    }
    
    @Test
    public void testEmbeddedMapUpdateOperationsOnInterfaceValue() throws Exception {
        morphia.map(ContainsMapOfEmbeddedGoos.class).map(ContainsMapOfEmbeddedInterfaces.class);
    	Goo g1 = new Goo("Scott");
    	Goo g2 = new Goo("Ralph");
    	    	
    	ContainsMapOfEmbeddedInterfaces cmoei = new ContainsMapOfEmbeddedInterfaces();
    	cmoei.values.put("first", g1);
    	ds.save(cmoei);
    	ds.update(cmoei, ds.createUpdateOperations(ContainsMapOfEmbeddedInterfaces.class).set("values.second", g2));
    	//check className in the map values.
    	BasicDBObject goo = (BasicDBObject) ( (BasicDBObject) ds.getCollection(ContainsMapOfEmbeddedInterfaces.class).findOne().get("values") ).get("second");
    	boolean hasF = goo.containsField(Mapper.CLASS_NAME_FIELDNAME);
    	assertTrue("className should be here.", hasF);
    }

    @Test //@Ignore("waiting on issue 184")
    public void testEmbeddedMapUpdateOperations() throws Exception {
        morphia.map(ContainsMapOfEmbeddedGoos.class).map(ContainsMapOfEmbeddedInterfaces.class);
    	Goo g1 = new Goo("Scott");
    	Goo g2 = new Goo("Ralph");
    	
    	ContainsMapOfEmbeddedGoos cmoeg = new ContainsMapOfEmbeddedGoos();
    	cmoeg.values.put("first", g1);
    	ds.save(cmoeg);
    	ds.update(cmoeg, ds.createUpdateOperations(ContainsMapOfEmbeddedGoos.class).set("values.second", g2));
    	//check className in the map values.
    	
    	BasicDBObject goo = (BasicDBObject) ( (BasicDBObject) ds.getCollection(ContainsMapOfEmbeddedGoos.class).findOne().get("values") ).get("second");
    	boolean hasF = goo.containsField(Mapper.CLASS_NAME_FIELDNAME);
    	assertTrue("className should not be here.", !hasF);
    }
}
