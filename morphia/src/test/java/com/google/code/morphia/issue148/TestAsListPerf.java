package com.google.code.morphia.issue148;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.mapping.cache.DefaultEntityCache;
import com.google.code.morphia.mapping.cache.EntityCache;
import com.google.code.morphia.query.Query;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

@SuppressWarnings({"unchecked", "unused"})
@Ignore("enable when testing on issue")
public class TestAsListPerf extends TestBase {

//	static {
//		MorphiaLoggerFactory.registerLogger((Class<? extends LogrFactory>) SilentLogger.class);
//	}
	
	final int nbOfAddresses = 500;
	final int nbOfTasks = 200;
	final int threadPool = 10;
	
	@Override
	public void cleanup() {
		//do nothing...
	}
	
	@Override
	public void setUp() {
		super.setUp();
        morphia.map(Address.class);
        if (this.ds.getCount(Address.class) == 0) {
			for (int i = 0; i < nbOfAddresses; i++) {
				Address address = new Address(i);
				ds.save(address);	
			}
			ds.find(Address.class).filter("name", "random").limit(-1).fetch();
        }
	}

	@Test
	public void compareDriverAndMorphiaQueryingOnce() throws Exception {
		double driverAvg = driverQueryAndMorphiaConv(nbOfAddresses, ds, morphia);
		double morphiaAvg = morphiaQueryAndMorphiaConv(nbOfAddresses, ds, morphia);
		System.out.println(String.format("compareDriverAndMorphiaQueryingOnce - driver: %4.2f ms/pojo , morphia: %4.2f ms/pojo ", driverAvg , morphiaAvg));
		Assert.assertNotNull(driverAvg);
	}
	
	@Test
	public void morphiaQueryingMultithreaded() throws InterruptedException {
		Result morphiaQueryThreadsResult = new Result(nbOfTasks);
		List<MorphiaQueryThread> morphiaThreads = new ArrayList<MorphiaQueryThread>(nbOfTasks);
		for (int i = 0; i < nbOfTasks; i++) {
			morphiaThreads.add(new MorphiaQueryThread(morphiaQueryThreadsResult, nbOfAddresses, ds, morphia));
		}
		ExecutorService morphiaPool = Executors.newFixedThreadPool(threadPool);
		for (MorphiaQueryThread thread : morphiaThreads) {
			morphiaPool.execute(thread);
		}
		morphiaPool.shutdown();
		morphiaPool.awaitTermination(30, TimeUnit.SECONDS);
		
		System.out.println(String.format("morphiaQueryingMultithreaded - (%d queries) morphia: %4.2f ms/pojo", 
				morphiaQueryThreadsResult.results.size(), 
				morphiaQueryThreadsResult.getAverageTime()));
	}
	
	@Test
	public void driverQueryingMultithreaded() throws InterruptedException {
		Result mongoQueryThreadsResult = new Result(nbOfTasks);
		List<MongoQueryThread> mongoThreads = new ArrayList<MongoQueryThread>(nbOfTasks);
		for (int i = 0; i < nbOfTasks; i++) {
			mongoThreads.add(new MongoQueryThread(mongoQueryThreadsResult, nbOfAddresses, ds, morphia));
		}
		ExecutorService mongoPool = Executors.newFixedThreadPool(threadPool);
		for (MongoQueryThread mongoQueryThread : mongoThreads) {
			mongoPool.execute(mongoQueryThread);
		}
		
		mongoPool.shutdown();
		mongoPool.awaitTermination(30, TimeUnit.SECONDS);
		
		System.out.println(String.format("driverQueryingMultithreaded - (%d queries) driver: %4.2f ms/pojo", 
				mongoQueryThreadsResult.results.size(), 
				mongoQueryThreadsResult.getAverageTime()));
		
	}
	
	@Test
	public void compareMorphiaAndDriverQueryingMultithreaded() throws InterruptedException {
		Result morphiaQueryThreadsResult = new Result(nbOfTasks);
		List<MorphiaQueryThread> morphiaThreads = new ArrayList<MorphiaQueryThread>(nbOfTasks);
		for (int i = 0; i < nbOfTasks; i++) {
			morphiaThreads.add(new MorphiaQueryThread(morphiaQueryThreadsResult, nbOfAddresses, ds, morphia));
		}
		ExecutorService morphiaPool = Executors.newFixedThreadPool(threadPool);
		for (MorphiaQueryThread thread : morphiaThreads) {
			morphiaPool.execute(thread);
		}
		
		morphiaPool.shutdown();
		morphiaPool.awaitTermination(30, TimeUnit.SECONDS);
		
		Result mongoQueryThreadsResult = new Result(nbOfTasks);
		List<MongoQueryThread> mongoThreads = new ArrayList<MongoQueryThread>(nbOfTasks);
		for (int i = 0; i < nbOfTasks; i++) {
			mongoThreads.add(new MongoQueryThread(mongoQueryThreadsResult, nbOfAddresses, ds, morphia));
		}
		ExecutorService mongoPool = Executors.newFixedThreadPool(threadPool);
		for (MongoQueryThread mongoQueryThread : mongoThreads) {
			mongoPool.execute(mongoQueryThread);
		}
		
		mongoPool.shutdown();
		mongoPool.awaitTermination(30, TimeUnit.SECONDS);
		
		System.out.println(String.format("compareMorphiaAndDriverQueryingMultithreaded (%d queries each) - driver: %4.2f ms/pojo (avg), morphia: %4.2f ms/pojo (avg)", 
				mongoQueryThreadsResult.results.size(), mongoQueryThreadsResult.getAverageTime(), morphiaQueryThreadsResult.getAverageTime()));
	}
	
	@Test
	public void compareDriverAndMorphiaQueryingMultithreaded() throws InterruptedException {
		Result mongoQueryThreadsResult = new Result(nbOfTasks);
		List<MongoQueryThread> mongoThreads = new ArrayList<MongoQueryThread>(nbOfTasks);
		for (int i = 0; i < nbOfTasks; i++) {
			mongoThreads.add(new MongoQueryThread(mongoQueryThreadsResult, nbOfAddresses, ds, morphia));
		}
		ExecutorService mongoPool = Executors.newFixedThreadPool(threadPool);
		for (MongoQueryThread mongoQueryThread : mongoThreads) {
			mongoPool.execute(mongoQueryThread);
		}
		
		mongoPool.shutdown();
		mongoPool.awaitTermination(30, TimeUnit.SECONDS);
		
		Result morphiaQueryThreadsResult = new Result(nbOfTasks);
		List<MorphiaQueryThread> morphiaThreads = new ArrayList<MorphiaQueryThread>(nbOfTasks);
		for (int i = 0; i < nbOfTasks; i++) {
			morphiaThreads.add(new MorphiaQueryThread(morphiaQueryThreadsResult, nbOfAddresses, ds, morphia));
		}
		ExecutorService morphiaPool = Executors.newFixedThreadPool(threadPool);
		for (MorphiaQueryThread thread : morphiaThreads) {
			morphiaPool.execute(thread);
		}
		morphiaPool.shutdown();
		morphiaPool.awaitTermination(30, TimeUnit.SECONDS);
		System.out.println(String.format("compareDriverAndMorphiaQueryingMultithreaded (%d queries each) - driver: %4.2f ms/pojo (avg), morphia %4.2f ms/pojo (avg)", 
				mongoQueryThreadsResult.results.size(), mongoQueryThreadsResult.getAverageTime(), morphiaQueryThreadsResult.getAverageTime()));
	}
	
	static class Result {
		
		private final Vector<Double> results;
		
		public Result(final int nbOfHits) {
			results = new Vector<Double>(nbOfHits);
		}
		
		public double getAverageTime() {
			Double total = 0d;
			for (Double duration : results) {
				total += duration;
			}
			return total / results.size();
		}
		
	}
	
	static class MorphiaQueryThread implements Runnable {
		private final Result result;
		private final int nbOfHits;
		private final Datastore ds;
		private final Morphia morphia;
		
		public MorphiaQueryThread(final Result result, final int nbOfHits, final Datastore ds, final Morphia morphia) {
			this.result = result;
			this.nbOfHits = nbOfHits;
			this.ds = ds;
			this.morphia = morphia;
		}
		
		public void run() {
			result.results.add(morphiaQueryAndMorphiaConv(nbOfHits, ds, morphia));
		}
	}
	
	static class MongoQueryThread implements Runnable {
		
		private final Result result;
		private final int nbOfHits;
		private final Datastore ds;
		private final Morphia morphia;
		
		public MongoQueryThread(final Result result, final int nbOfHits, final Datastore ds, final Morphia morphia) {
			this.result = result;
			this.nbOfHits = nbOfHits;
			this.ds = ds;
			this.morphia = morphia;
		}
		
		public void run() {
			result.results.add(driverQueryAndMorphiaConv(nbOfHits, ds, morphia));
		}
	}
	
	public static double morphiaQueryAndMorphiaConv(final int nbOfHits, final Datastore ds, final Morphia morphia) {
		Query<Address> query = ds.createQuery(Address.class).
									order("name");
		long start = System.nanoTime();
		List<Address> resultList = query.asList();
		long duration = (System.nanoTime() - start) / 1000000; //ns -> ms
		Assert.assertEquals(nbOfHits, resultList.size());
		return (double)duration/nbOfHits;
	}
	
	public static double driverQueryAndMorphiaConv(final int nbOfHits, final Datastore ds, final Morphia morphia) {
		long start = System.nanoTime();
		List<DBObject> list = ds.getDB().getCollection("Address").
									find().
									sort(new BasicDBObject("name", 1)).
									toArray();
		EntityCache entityCache = new DefaultEntityCache();
		List<Address> resultList = new LinkedList<Address>();
		for (DBObject dbObject : list) {
			Address address = morphia.fromDBObject(Address.class, dbObject, entityCache);
			resultList.add(address);
		}
		long duration = (System.nanoTime() - start) / 1000000; //ns -> ms
		Assert.assertEquals(nbOfHits, resultList.size());
		return (double)duration/nbOfHits;
	}

	@Entity
	private static class Address {
		
		public Address() {
			
		}
		
		public Address(final int i) {
			parity = i % 2 == 0 ? 1 : 0;
			name += i;
			street += i;
			city += i;
			state += i;
			zip += i;
		}
		
		@Id
		ObjectId id;
		int parity;
		String name = "Scott";
		String street = "3400 Maple";
		String city = "Manhattan Beach";
		String state = "CA";
		int zip = 94114;
		Date added = new Date();
	}
}
