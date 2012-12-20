/**
 * 
 */
package com.google.code.morphia;

import org.junit.After;
import org.junit.Before;

import com.google.code.morphia.mapping.MappedClass;
import com.mongodb.DB;
import com.mongodb.Mongo;

public abstract class TestBase
{
    protected Mongo mongo;
    protected DB db;
    protected Datastore ds;
    protected AdvancedDatastore ads;
    protected Morphia morphia = new Morphia();

    protected TestBase() {
        try {
			this.mongo = new Mongo();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
    
    @Before
    public void setUp()
    {
        this.db = this.mongo.getDB("morphia_test");
        this.ds = this.morphia.createDatastore(this.mongo, this.db.getName());
        this.ads = (AdvancedDatastore) ds;
        //ads.setDecoderFact(LazyDBDecoder.FACTORY);
    }
	
    protected void cleanup() {
        //this.mongo.dropDatabase("morphia_test");
		for(MappedClass mc : morphia.getMapper().getMappedClasses())
//			if( mc.getEntityAnnotation() != null )
				db.getCollection(mc.getCollectionName()).drop();
    	
    }
    
	@After
	public void tearDown() {
    	cleanup();
		mongo.close();
	}
}
