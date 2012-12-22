package com.google.code.morphia.query;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;

/**
 * @author scotthernandez
 */
@SuppressWarnings("unused")
public class QueryInId extends TestBase {

	@Entity("docs")
	static private class Doc {
		@Id
		public long id = 4;
	}

	@Test
	public void testInIdList() throws Exception {
		Doc doc = new Doc();
		doc.id = 1;
		ds.save(doc);

		// this works
		List<Doc> docs1 = ds.find(Doc.class).field("_id").equal(1).asList();

		List<Long> idList = new ArrayList<Long>();
		idList.add(1L);
		// this causes an NPE
		List<Doc> docs2 = ds.find(Doc.class).field("_id").in(idList).asList();

	}	

}
