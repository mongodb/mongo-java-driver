/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia.issue148;

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
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({ "unchecked", "unused" })
@Ignore("enable when testing on issue")
public class AsListPerfTest extends TestBase {

//    static {
//        MorphiaLoggerFactory.registerLogger((Class<? extends LogrFactory>) SilentLogger.class);
//    }

    private final int nbOfAddresses = 500;
    private final int nbOfTasks = 200;
    private final int threadPool = 10;

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
                final Address address = new Address(i);
                ds.save(address);
            }
            ds.find(Address.class).filter("name", "random").limit(-1).fetch();
        }
    }

    @Test
    public void compareDriverAndMorphiaQueryingOnce() throws Exception {
        final double driverAvg = driverQueryAndMorphiaConv(nbOfAddresses, ds, morphia);
        final double morphiaAvg = morphiaQueryAndMorphiaConv(nbOfAddresses, ds, morphia);
        System.out.println(String.format("compareDriverAndMorphiaQueryingOnce - driver: %4.2f ms/pojo , "
                                                 + "morphia: %4.2f ms/pojo ", driverAvg, morphiaAvg));
        Assert.assertNotNull(driverAvg);
    }

    @Test
    public void morphiaQueryingMultithreaded() throws InterruptedException {
        final Result morphiaQueryThreadsResult = new Result(nbOfTasks);
        final List<MorphiaQueryThread> morphiaThreads = new ArrayList<MorphiaQueryThread>(nbOfTasks);
        for (int i = 0; i < nbOfTasks; i++) {
            morphiaThreads.add(new MorphiaQueryThread(morphiaQueryThreadsResult, nbOfAddresses, ds, morphia));
        }
        final ExecutorService morphiaPool = Executors.newFixedThreadPool(threadPool);
        for (final MorphiaQueryThread thread : morphiaThreads) {
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
        final Result mongoQueryThreadsResult = new Result(nbOfTasks);
        final List<MongoQueryThread> mongoThreads = new ArrayList<MongoQueryThread>(nbOfTasks);
        for (int i = 0; i < nbOfTasks; i++) {
            mongoThreads.add(new MongoQueryThread(mongoQueryThreadsResult, nbOfAddresses, ds, morphia));
        }
        final ExecutorService mongoPool = Executors.newFixedThreadPool(threadPool);
        for (final MongoQueryThread mongoQueryThread : mongoThreads) {
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
        final Result morphiaQueryThreadsResult = new Result(nbOfTasks);
        final List<MorphiaQueryThread> morphiaThreads = new ArrayList<MorphiaQueryThread>(nbOfTasks);
        for (int i = 0; i < nbOfTasks; i++) {
            morphiaThreads.add(new MorphiaQueryThread(morphiaQueryThreadsResult, nbOfAddresses, ds, morphia));
        }
        final ExecutorService morphiaPool = Executors.newFixedThreadPool(threadPool);
        for (final MorphiaQueryThread thread : morphiaThreads) {
            morphiaPool.execute(thread);
        }

        morphiaPool.shutdown();
        morphiaPool.awaitTermination(30, TimeUnit.SECONDS);

        final Result mongoQueryThreadsResult = new Result(nbOfTasks);
        final List<MongoQueryThread> mongoThreads = new ArrayList<MongoQueryThread>(nbOfTasks);
        for (int i = 0; i < nbOfTasks; i++) {
            mongoThreads.add(new MongoQueryThread(mongoQueryThreadsResult, nbOfAddresses, ds, morphia));
        }
        final ExecutorService mongoPool = Executors.newFixedThreadPool(threadPool);
        for (final MongoQueryThread mongoQueryThread : mongoThreads) {
            mongoPool.execute(mongoQueryThread);
        }

        mongoPool.shutdown();
        mongoPool.awaitTermination(30, TimeUnit.SECONDS);

        System.out.println(String.format("compareMorphiaAndDriverQueryingMultithreaded (%d queries each) - driver: %4"
                                                 + ".2f ms/pojo (avg), morphia: %4.2f ms/pojo (avg)",
                                         mongoQueryThreadsResult.results.size(),
                                         mongoQueryThreadsResult.getAverageTime(),
                                         morphiaQueryThreadsResult.getAverageTime()));
    }

    @Test
    public void compareDriverAndMorphiaQueryingMultithreaded() throws InterruptedException {
        final Result mongoQueryThreadsResult = new Result(nbOfTasks);
        final List<MongoQueryThread> mongoThreads = new ArrayList<MongoQueryThread>(nbOfTasks);
        for (int i = 0; i < nbOfTasks; i++) {
            mongoThreads.add(new MongoQueryThread(mongoQueryThreadsResult, nbOfAddresses, ds, morphia));
        }
        final ExecutorService mongoPool = Executors.newFixedThreadPool(threadPool);
        for (final MongoQueryThread mongoQueryThread : mongoThreads) {
            mongoPool.execute(mongoQueryThread);
        }

        mongoPool.shutdown();
        mongoPool.awaitTermination(30, TimeUnit.SECONDS);

        final Result morphiaQueryThreadsResult = new Result(nbOfTasks);
        final List<MorphiaQueryThread> morphiaThreads = new ArrayList<MorphiaQueryThread>(nbOfTasks);
        for (int i = 0; i < nbOfTasks; i++) {
            morphiaThreads.add(new MorphiaQueryThread(morphiaQueryThreadsResult, nbOfAddresses, ds, morphia));
        }
        final ExecutorService morphiaPool = Executors.newFixedThreadPool(threadPool);
        for (final MorphiaQueryThread thread : morphiaThreads) {
            morphiaPool.execute(thread);
        }
        morphiaPool.shutdown();
        morphiaPool.awaitTermination(30, TimeUnit.SECONDS);
        System.out.println(String.format("compareDriverAndMorphiaQueryingMultithreaded (%d queries each) - driver: %4"
                                                 + ".2f ms/pojo (avg), morphia %4.2f ms/pojo (avg)",
                                         mongoQueryThreadsResult.results.size(),
                                         mongoQueryThreadsResult.getAverageTime(),
                                         morphiaQueryThreadsResult.getAverageTime()));
    }

    static class Result {

        private final Vector<Double> results;

        public Result(final int nbOfHits) {
            results = new Vector<Double>(nbOfHits);
        }

        public double getAverageTime() {
            Double total = 0d;
            for (final Double duration : results) {
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
        final Query<Address> query = ds.createQuery(Address.class).
                order("name");
        final long start = System.nanoTime();
        final List<Address> resultList = query.asList();
        final long duration = (System.nanoTime() - start) / 1000000; //ns -> ms
        Assert.assertEquals(nbOfHits, resultList.size());
        return (double) duration / nbOfHits;
    }

    public static double driverQueryAndMorphiaConv(final int nbOfHits, final Datastore ds, final Morphia morphia) {
        final long start = System.nanoTime();
        final List<DBObject> list = ds.getDB().getCollection("Address").
                find().
                sort(new BasicDBObject("name", 1)).
                toArray();
        final EntityCache entityCache = new DefaultEntityCache();
        final List<Address> resultList = new LinkedList<Address>();
        for (final DBObject dbObject : list) {
            final Address address = morphia.fromDBObject(Address.class, dbObject, entityCache);
            resultList.add(address);
        }
        final long duration = (System.nanoTime() - start) / 1000000; //ns -> ms
        Assert.assertEquals(nbOfHits, resultList.size());
        return (double) duration / nbOfHits;
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
        private ObjectId id;
        private int parity;
        private String name = "Scott";
        private String street = "3400 Maple";
        private String city = "Manhattan Beach";
        private String state = "CA";
        private int zip = 94114;
        private final Date added = new Date();
    }
}
