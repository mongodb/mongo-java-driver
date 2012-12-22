package com.google.code.morphia.mapping;

import java.util.List;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;

/**
 * @author scotthernandez
 */
public class ReferencesWIgnoreMissingTests extends TestBase
{
    @Entity
    static class Container {
    	public @Id ObjectId id;
		@Reference(ignoreMissing=true) private StringHolder[] refs = null;
    }

    @Entity
    static class StringHolder {
    	@Id ObjectId id = new ObjectId();
    }
    
    @Test
    public void TestMissingReference() throws Exception {
    	Container c = new Container();
    	c.refs = new StringHolder[] {new StringHolder(), new StringHolder()};
    	ds.save(c);
    	ds.save(c.refs[0]);
    	
    	Container reloadedContainer = ds.find(Container.class).get();
    	Assert.assertNotNull(reloadedContainer);
    	Assert.assertNotNull(reloadedContainer.refs);
    	Assert.assertEquals(1,reloadedContainer.refs.length);

    	reloadedContainer = ds.get(c);
        Assert.assertNotNull(reloadedContainer);
    	Assert.assertNotNull(reloadedContainer.refs);
    	Assert.assertEquals(1,reloadedContainer.refs.length);
    	
    	List<Container> cs = ds.find(Container.class).asList();
    	Assert.assertNotNull(cs);
    	Assert.assertEquals(1, cs.size());
   	
    }
}
