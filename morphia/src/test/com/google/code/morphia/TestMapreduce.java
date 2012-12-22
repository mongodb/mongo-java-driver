package com.google.code.morphia;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.testmodel.Circle;
import com.google.code.morphia.testmodel.Rectangle;
import com.google.code.morphia.testmodel.Shape;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TestMapreduce extends TestBase{

	@Entity("mr-results")
	private static class ResultEntity extends ResultBase<String, HasCount> {
	}
	
	private static class ResultBase<T,V> {
		@Id T type;
		@Embedded
		V value;
	}
	
	private static class HasCount {
		double count;
	}
	
	@SuppressWarnings("unused")
	@Entity("mr-results")
	private static class ResultEntity2{
		@Id String type;
		double count;
		
		@PreLoad void preLoad(BasicDBObject dbObj){
			//pull all the fields from value field into the parent.
			dbObj.putAll((DBObject)dbObj.get("value"));
		}
	}
	
	
	@Test
    public void testMR() throws Exception {
	
		Random rnd = new Random();
		
		//create 100 circles and rectangles
		for(int i = 0; i < 100; i++){
			ads.insert("shapes", new Circle(rnd.nextDouble()));
			ads.insert("shapes", new Rectangle(rnd.nextDouble(), rnd.nextDouble()));
		}
		String map = "function () { if(this['radius']) { emit('circle', {count:1}); return; } emit('rect', {count:1}); }";
		String reduce = "function (key, values) { var total = 0; for ( var i=0; i<values.length; i++ ) {total += values[i].count;} return { count : total }; }";
		MapreduceResults<ResultEntity> mrRes = ds.mapReduce(MapreduceType.REPLACE, ads.createQuery(Shape.class), map, reduce, null, null, ResultEntity.class);
		Assert.assertEquals(2, mrRes.createQuery().countAll());
		Assert.assertEquals(100, mrRes.createQuery().get().value.count,0);
	}
	
}
