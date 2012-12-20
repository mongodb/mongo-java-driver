package com.google.code.morphia.query;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;

/**
 * @author scotthernandez
 */
@SuppressWarnings("unused")
public class QueryInForReferencedList extends TestBase {

	@Entity
	private static class HasRefs {
		private static final long serialVersionUID = 1L;

		@Id
		ObjectId id = new ObjectId();
		@Reference
		ArrayList<ReferencedEntity> refs = new ArrayList<QueryInForReferencedList.ReferencedEntity>();
	}

	@Entity
	private static class ReferencedEntity extends TestEntity {
		private static final long serialVersionUID = 1L;
		String foo;

		public ReferencedEntity(String s) {
			foo = s;
		}

		public ReferencedEntity() {
		}

	}

	@Entity("docs")
	static private class Doc {
		@Id
		public long id = 4;
	}

	@Test
	public void testMapping() throws Exception {

		morphia.map(HasRefs.class);
		morphia.map(ReferencedEntity.class);
	}

	@Test
	public void testInQuery() throws Exception {
		HasRefs hr = new HasRefs();
		for (int x = 0; x < 10; x++) {
			ReferencedEntity re = new ReferencedEntity("" + x);
			hr.refs.add(re);
		}
		ds.save(hr.refs);
		ds.save(hr);

		List<HasRefs> res = ds.createQuery(HasRefs.class).field("refs")
				.in(hr.refs.subList(1, 3)).asList();
		Assert.assertEquals(1, res.size());
	}
	
	@Test
	public void testInQuery2() throws Exception {
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
