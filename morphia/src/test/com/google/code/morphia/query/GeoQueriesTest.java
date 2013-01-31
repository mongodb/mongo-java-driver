/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.query;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Indexed;
import com.google.code.morphia.utils.IndexDirection;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class GeoQueriesTest extends TestBase {
    @SuppressWarnings("unused")
    @Entity
    private static class Place {
        @Id
        private ObjectId id;
        private String name = "";
        @Indexed(IndexDirection.GEO2D)
        private double[] loc = null;

        public Place(final String name, final double[] loc) {
            this.name = name;
            this.loc = loc;
        }

        private Place() {
        }
    }

    @Override
    public void setUp() {
        super.setUp();
        morphia.map(Place.class);
    }

    @Test
    public void testNear() throws Exception {
        ds.ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        final Place found = ds.find(Place.class).field("loc").near(0, 0).get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testNearMaxDistance() throws Exception {
        ds.ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        final Place found = ds.find(Place.class).field("loc").near(0, 0, 1.5).get();
        Assert.assertNotNull(found);
        final Place notFound = ds.find(Place.class).field("loc").near(0, 0, 1).get();
        Assert.assertNull(notFound);
    }

    @Test
    public void testNearNoIndex() throws Exception {
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        Place found = null;
        try {
            found = ds.find(Place.class).field("loc").near(0, 0).get();
            Assert.assertFalse(true);
        } catch (MongoInternalException e) {
            Assert.assertNull(found);
        } catch (MongoException e) {
            Assert.assertNull(found);
        }
    }

    @Test
    public void testWithinRadius() throws Exception {
        ds.ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        final Place found = ds.find(Place.class).field("loc").within(0, 1, 1.1).get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testWithinRadius2() throws Exception {
        ds.ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        final Place found = ds.find(Place.class).field("loc").within(0.5, 0.5, 0.77).get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testWithinRadiusSphere() throws Exception {
        ds.ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        final Place found = ds.find(Place.class).field("loc").within(0, 1, 1, true).get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testWithinOutsideRadius() throws Exception {
        ds.ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        final Place found = ds.find(Place.class).field("loc").within(2, 2, .4).get();
        Assert.assertNull(found);
    }

    @Test
    public void testWithinBox() throws Exception {
        ds.ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        final Place found = ds.find(Place.class).field("loc").within(0, 0, 2, 2).get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testWithinOutsideBox() throws Exception {
        ds.ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        ds.save(place1);
        final Place found = ds.find(Place.class).field("loc").within(0, 0, .4, .5).get();
        Assert.assertNull(found);
    }
}
