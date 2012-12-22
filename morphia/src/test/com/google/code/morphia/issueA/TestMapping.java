package com.google.code.morphia.issueA;

import java.io.Serializable;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import com.mongodb.DBObject;


/**
 * Test from email to mongodb-users list.
 */
public class TestMapping extends TestBase {
    
	@Test
    public void testMapping() {
        morphia.map(Class_level_three.class);
        Class_level_three sp = new Class_level_three();

        //Old way
        DBObject wrapObj = morphia.toDBObject(sp);  //the error points here from the user
        ds.getDB().getCollection("testColl").save(wrapObj);
        
        
        //better way
        ds.save(sp);
        
    }

    private static interface Interface_one<K> {
    	K getK();
    }
    
    private static class Class_level_one <K> implements Interface_one<K>, Cloneable, Serializable{
    	K k;
    	public K getK() {
    		return k;
    	}
    }

    private static class Class_level_two extends Class_level_one<String>{

    }

    private static class Class_level_three{
            @Id
            private ObjectId id;

            private String name;

            @Embedded
            private Class_level_two value;
    }


}
