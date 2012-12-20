/**
 *
 */
package com.google.code.morphia.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.mongodb.DBObject;

/**
 * @author scott hernandez
 */
public class MapperOptionsTest extends TestBase{

	@SuppressWarnings("unused")
	private static class HasList implements Serializable {
		static final long serialVersionUID = 1L;
		@Id ObjectId id = new ObjectId();
		List<String> names = new ArrayList<String>();


		HasList(){}
		HasList(String n) { if(n==null) names = null; else names.add(n); }
	}


	@Test
	public void emptyListStoredWithOptions() throws Exception {
		HasList hl = new HasList();

		//Test default behavior
		ds.save(hl);
		DBObject dbObj =  ds.getCollection(HasList.class).findOne();
		Assert.assertFalse("field exists, value =" + dbObj.get("names"), dbObj.containsField("names"));

		//Test default storing empty list/array with storeEmpties option
		this.morphia.getMapper().getOptions().storeEmpties = true;
		ds.save(hl);
		dbObj =  ds.getCollection(HasList.class).findOne();
		Assert.assertTrue("field missing", dbObj.containsField("names"));

		//Test opposite from above
		this.morphia.getMapper().getOptions().storeEmpties = false;
		ds.save(hl);
		dbObj =  ds.getCollection(HasList.class).findOne();
		Assert.assertFalse("field exists, value =" + dbObj.get("names"), dbObj.containsField("names"));
	}
}
