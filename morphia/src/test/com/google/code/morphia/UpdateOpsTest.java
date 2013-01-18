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

package com.google.code.morphia;

import com.google.code.morphia.QueryTest.ContainsPic;
import com.google.code.morphia.QueryTest.Pic;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateResults;
import com.google.code.morphia.query.ValidationException;
import com.google.code.morphia.testmodel.Circle;
import com.google.code.morphia.testmodel.Rectangle;
import com.google.code.morphia.testutil.StandardTests;
import com.mongodb.WriteConcern;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Scott Hernandez
 */
@SuppressWarnings("unused")
public class UpdateOpsTest extends TestBase {

    private static class ContainsIntArray {
        @Id
        private ObjectId id;
        private final Integer[] vals = {1, 2, 3};
    }

    private static class ContainsInt {
        @Id
        private ObjectId id;
        private int val;
    }

    @Entity
    private static class ContainsPicKey {
        @Id
        private ObjectId id;
        private String name = "test";
        private Key<Pic> pic;
    }

    @Test
    @Category(StandardTests.class)
    public void testIncDec() throws Exception {
        final Rectangle[] rects = {new Rectangle(1, 10),
                new Rectangle(1, 10),
                new Rectangle(1, 10),
                new Rectangle(10, 10),
                new Rectangle(10, 10),
        };
        for (final Rectangle rect : rects) {
            ds.save(rect);
        }

        final Query<Rectangle> q1 = ds.find(Rectangle.class, "height", 1D);
        final Query<Rectangle> q2 = ds.find(Rectangle.class, "height", 2D);

        assertEquals(3, ds.getCount(q1));
        assertEquals(0, ds.getCount(q2));

        final UpdateResults<Rectangle> results = ds.update(q1,
                                                           ds.createUpdateOperations(Rectangle.class).inc("height"));
        assertUpdated(results, 3);

        assertEquals(0, ds.getCount(q1));
        assertEquals(3, ds.getCount(q2));

        ds.update(q2, ds.createUpdateOperations(Rectangle.class).dec("height"));
        assertEquals(3, ds.getCount(q1));
        assertEquals(0, ds.getCount(q2));

        ds.update(ds.find(Rectangle.class, "width", 1D),
                  ds.createUpdateOperations(Rectangle.class).set("height", 1D).set("width", 1D), true);
        assertNotNull(ds.find(Rectangle.class, "width", 1D).get());
        assertNull(ds.find(Rectangle.class, "width", 2D).get());
        ds.update(ds.find(Rectangle.class, "width", 1D),
                  ds.createUpdateOperations(Rectangle.class).set("height", 2D).set("width", 2D), true);
        assertNull(ds.find(Rectangle.class, "width", 1D).get());
        assertNotNull(ds.find(Rectangle.class, "width", 2D).get());
    }

    @Test
    public void testInsertUpdate() throws Exception {
        final UpdateResults<Circle> res = ds.update(ds.createQuery(Circle.class).field("radius").equal(0),
                                                    ds.createUpdateOperations(Circle.class).inc("radius", 1D), true);
        assertInserted(res);
    }

    @Test
    public void testSetUnset() throws Exception {
        final Key<Circle> key = ds.save(new Circle(1));

        UpdateResults<Circle> res = ds.updateFirst(
                ds.find(Circle.class, "radius", 1D),
                ds.createUpdateOperations(Circle.class).set("radius", 2D));

        assertUpdated(res, 1);

        final Circle c = ds.getByKey(Circle.class, key);
        assertEquals(2D, c.getRadius(), 0);


        res = ds.updateFirst(
                ds.find(Circle.class, "radius", 2D),
                ds.createUpdateOperations(Circle.class).unset("radius"));
        assertUpdated(res, 1);

        final Circle c2 = ds.getByKey(Circle.class, key);
        assertEquals(0D, c2.getRadius(), 0);
    }


    @Test(expected = ValidationException.class)
    public void testValidationBadFieldName() throws Exception {
        ds.update(
                ds.createQuery(Circle.class).field("radius").equal(0),
                ds.createUpdateOperations(Circle.class).inc("r", 1D), true, WriteConcern.SAFE);
        Assert.assertTrue(false); //should not get here.
    }

    @Test
    @Ignore("need to inspect the logs")
    public void testValidationBadFieldType() throws Exception {
        try {
            ds.update(
                    ds.createQuery(Circle.class).field("radius").equal(0),
                    ds.createUpdateOperations(Circle.class).set("radius", "1"), true, WriteConcern.SAFE);
            Assert.assertTrue(false); //should not get here.
        } catch (ValidationException e) {
            Assert.assertTrue(e.getMessage().contains("inconsistent"));
        }
    }

    @Test
    @Ignore("Ignore until driver support getlasterror after the fact")
    // TODO: Un-ignore this if driver decides to support this
    public void testInsertUpdatesUnsafe() throws Exception {
        final UpdateResults<Circle> res = ds.update(ds.createQuery(Circle.class).field("radius").equal(0),
                                                    ds.createUpdateOperations(Circle.class).inc("radius", 1D), true,
                                                    WriteConcern.NONE);
        assertInserted(res);
    }

    @Test
    public void testUpdateWithDifferentType() throws Exception {
        final ContainsInt cInt = new ContainsInt();
        cInt.val = 21;
        ds.save(cInt);

        final UpdateResults<ContainsInt> res = ds.updateFirst(ds.createQuery(ContainsInt.class),
                                                              ds.createUpdateOperations(ContainsInt.class).inc("val",
                                                                                                               1.1D));
        assertUpdated(res, 1);

        final ContainsInt ciLoaded = ds.find(ContainsInt.class).limit(1).get();
        assertEquals(22, ciLoaded.val);
    }

    @Test
    public void testRemoveFirst() throws Exception {
        final ContainsIntArray cIntArray = new ContainsIntArray();
        ds.save(cIntArray);
        ContainsIntArray cIALoaded = ds.get(cIntArray);
        assertEquals(3, cIALoaded.vals.length);
        assertArrayEquals((new ContainsIntArray()).vals, cIALoaded.vals);

        //remove 1
        UpdateResults<ContainsIntArray> res = ds.updateFirst(
                ds.createQuery(ContainsIntArray.class),
                ds.createUpdateOperations(ContainsIntArray.class).removeFirst("vals"));
        assertUpdated(res, 1);
        cIALoaded = ds.get(cIntArray);
        assertArrayEquals(new Integer[]{2, 3}, cIALoaded.vals);

        //remove 3
        res = ds.updateFirst(
                ds.createQuery(ContainsIntArray.class),
                ds.createUpdateOperations(ContainsIntArray.class).removeLast("vals"));
        assertUpdated(res, 1);
        cIALoaded = ds.get(cIntArray);
        assertArrayEquals(new Integer[]{2}, cIALoaded.vals);
    }

    @SuppressWarnings("rawtypes")
    private void assertUpdated(final UpdateResults res, final int count) {
        assertEquals(0, res.getInsertedCount());
        assertEquals(count, res.getUpdatedCount());
        assertEquals(true, res.getUpdatedExisting());
    }

    @SuppressWarnings("rawtypes")
    private void assertInserted(final UpdateResults res) {
        assertEquals(1, res.getInsertedCount());
        assertEquals(0, res.getUpdatedCount());
        assertEquals(false, res.getUpdatedExisting());
    }

    @Test
    public void testAdd() throws Exception {
        ContainsIntArray cIntArray = new ContainsIntArray();
        ds.save(cIntArray);
        ContainsIntArray cIALoaded = ds.get(cIntArray);
        assertEquals(3, cIALoaded.vals.length);
        assertArrayEquals((new ContainsIntArray()).vals, cIALoaded.vals);

        //add 4 to array
        UpdateResults<ContainsIntArray> res = ds.updateFirst(ds.createQuery(ContainsIntArray.class),
                                                             ds.createUpdateOperations(ContainsIntArray.class).
                                                                     add("vals", 4, false));
        assertUpdated(res, 1);

        cIALoaded = ds.get(cIntArray);
        assertArrayEquals(new Integer[]{1, 2, 3, 4}, cIALoaded.vals);

        //add unique (4) -- noop
        res = ds.updateFirst(ds.createQuery(ContainsIntArray.class),
                             ds.createUpdateOperations(ContainsIntArray.class).add("vals", 4, false));
        assertUpdated(res, 1);

        cIALoaded = ds.get(cIntArray);
        assertArrayEquals(new Integer[]{1, 2, 3, 4}, cIALoaded.vals);

        //add dup 4
        res = ds.updateFirst(ds.createQuery(ContainsIntArray.class),
                             ds.createUpdateOperations(ContainsIntArray.class).add("vals", 4, true));
        assertUpdated(res, 1);

        cIALoaded = ds.get(cIntArray);
        assertArrayEquals(new Integer[]{1, 2, 3, 4, 4}, cIALoaded.vals);

        //cleanup for next tests
        ds.delete(ds.find(ContainsIntArray.class));
        cIntArray = ds.getByKey(ContainsIntArray.class, ds.save(new ContainsIntArray()));

        //add [4,5]
        final List<Integer> newVals = new ArrayList<Integer>();
        newVals.add(4);
        newVals.add(5);
        res = ds.updateFirst(ds.createQuery(ContainsIntArray.class),
                             ds.createUpdateOperations(ContainsIntArray.class).addAll("vals", newVals, false));
        assertUpdated(res, 1);

        cIALoaded = ds.get(cIntArray);
        assertArrayEquals(new Integer[]{1, 2, 3, 4, 5}, cIALoaded.vals);

        //add them again... noop
        res = ds.updateFirst(ds.createQuery(ContainsIntArray.class),
                             ds.createUpdateOperations(ContainsIntArray.class).addAll("vals", newVals, false));
        assertUpdated(res, 1);

        cIALoaded = ds.get(cIntArray);
        assertArrayEquals(new Integer[]{1, 2, 3, 4, 5}, cIALoaded.vals);

        //add dups [4,5]
        res = ds.updateFirst(ds.createQuery(ContainsIntArray.class),
                             ds.createUpdateOperations(ContainsIntArray.class).addAll("vals", newVals, true));
        assertUpdated(res, 1);

        cIALoaded = ds.get(cIntArray);
        assertArrayEquals(new Integer[]{1, 2, 3, 4, 5, 4, 5}, cIALoaded.vals);

    }

    @Test
    public void testExistingUpdates() throws Exception {
        Circle c = new Circle(100D);
        ds.save(c);
        c = new Circle(12D);
        ds.save(c);
        UpdateResults<Circle> res = ds.updateFirst(ds.createQuery(Circle.class),
                                                   ds.createUpdateOperations(Circle.class).inc("radius", 1D));
        assertUpdated(res, 1);

        res = ds.update(ds.createQuery(Circle.class), ds.createUpdateOperations(Circle.class).inc("radius"));
        assertUpdated(res, 2);

        //test possible datatype change.
        final Circle cLoaded = ds.find(Circle.class, "radius", 13).get();
        assertNotNull(cLoaded);
        assertEquals(13D, cLoaded.getRadius(), 0D);
    }

    @Test
    @Ignore("waiting on SERVER-1470 bug; dbref is not included from query on upsert")
    public void testInsertWithRef() throws Exception {
        final Pic pic = new Pic();
        pic.name = "fist";
        final Key<Pic> picKey = ds.save(pic);

        //test with Key<Pic>
        UpdateResults<ContainsPic> res = ds.updateFirst(
                ds.find(ContainsPic.class, "name", "first").filter("pic", picKey),
                ds.createUpdateOperations(ContainsPic.class).set("name", "A"),
                true);

        assertInserted(res);
        assertEquals(1, ds.find(ContainsPic.class).countAll());

        ds.delete(ds.find(ContainsPic.class));

        //test with pic object
        res = ds.updateFirst(
                ds.find(ContainsPic.class, "name", "first").filter("pic", pic),
                ds.createUpdateOperations(ContainsPic.class).set("name", "second"),
                true);

        assertInserted(res);
        assertEquals(1, ds.find(ContainsPic.class).countAll());

        //test reading the object.
        final ContainsPic cp = ds.find(ContainsPic.class).get();
        assertNotNull(cp);
        assertEquals(cp.name, "second");
        assertNotNull(cp.pic);
        assertNotNull(cp.pic.name);
        assertEquals(cp.pic.name, "fist");

    }

    @Test
    public void testUpdateRef() throws Exception {
        final ContainsPic cp = new ContainsPic();
        cp.name = "cp one";

        final Key<ContainsPic> cpKey = ds.save(cp);

        final Pic pic = new Pic();
        pic.name = "fist";
        final Key<Pic> picKey = ds.save(pic);


        //test with Key<Pic>
        UpdateResults<ContainsPic> res = ds.updateFirst(
                ds.find(ContainsPic.class, "name", cp.name),
                ds.createUpdateOperations(ContainsPic.class).set("pic", pic));

        assertEquals(1, res.getUpdatedCount());

        //test reading the object.
        final ContainsPic cp2 = ds.find(ContainsPic.class).get();
        assertNotNull(cp2);
        assertEquals(cp2.name, cp.name);
        assertNotNull(cp2.pic);
        assertNotNull(cp2.pic.name);
        assertEquals(cp2.pic.name, pic.name);

        res = ds.updateFirst(
                ds.find(ContainsPic.class, "name", cp.name),
                ds.createUpdateOperations(ContainsPic.class).set("pic", picKey));

        //test reading the object.
        final ContainsPic cp3 = ds.find(ContainsPic.class).get();
        assertNotNull(cp3);
        assertEquals(cp3.name, cp.name);
        assertNotNull(cp3.pic);
        assertNotNull(cp3.pic.name);
        assertEquals(cp3.pic.name, pic.name);
    }

    @Test
    public void testUpdateKeyRef() throws Exception {
        final ContainsPicKey cpk = new ContainsPicKey();
        cpk.name = "cpk one";

        final Key<ContainsPicKey> cpKey = ds.save(cpk);

        final Pic pic = new Pic();
        pic.name = "fist again";
        final Key<Pic> picKey = ds.save(pic);
//        picKey = ds.getKey(pic);

        //test with Key<Pic>
        UpdateResults<ContainsPicKey> res = ds.updateFirst(
                ds.find(ContainsPicKey.class, "name", cpk.name),
                ds.createUpdateOperations(ContainsPicKey.class).set("pic", pic));

        assertEquals(1, res.getUpdatedCount());

        //test reading the object.
        final ContainsPicKey cpk2 = ds.find(ContainsPicKey.class).get();
        assertNotNull(cpk2);
        assertEquals(cpk2.name, cpk.name);
        assertNotNull(cpk2.pic);
        assertEquals(cpk2.pic, picKey);

        res = ds.updateFirst(
                ds.find(ContainsPicKey.class, "name", cpk.name),
                ds.createUpdateOperations(ContainsPicKey.class).set("pic", picKey));

        //test reading the object.
        final ContainsPicKey cpk3 = ds.find(ContainsPicKey.class).get();
        assertNotNull(cpk3);
        assertEquals(cpk3.name, cpk.name);
        assertNotNull(cpk3.pic);
        assertEquals(cpk3.pic, picKey);
    }

}
