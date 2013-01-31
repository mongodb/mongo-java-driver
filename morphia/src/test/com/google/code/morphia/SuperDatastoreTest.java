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

package com.google.code.morphia;

import com.google.code.morphia.testmodel.Rectangle;
import com.mongodb.BasicDBObject;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Scott Hernandez
 */
public class SuperDatastoreTest extends TestBase {

    private AdvancedDatastore ads;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        ads = (AdvancedDatastore) ds;
    }

    @Test
    public void testSaveAndDelete() throws Exception {
        final String ns = "hotels";
        final Rectangle rect = new Rectangle(10, 10);
        rect.setId("1");

        db.getCollection(ns).remove(new BasicDBObject());

        //test delete(entity, id)
        ads.save(ns, rect);
        assertEquals(1, ads.getCount(ns));
        ads.delete(ns, Rectangle.class, 1);
        assertEquals(1, ads.getCount(ns));
        ads.delete(ns, Rectangle.class, "1");
        assertEquals(0, ads.getCount(ns));
    }

    @Test
    public void testGet() throws Exception {
        final String ns = "hotels";
        final Rectangle rect = new Rectangle(10, 10);
        rect.setId("1");

        db.getCollection(ns).remove(new BasicDBObject());

        //test delete(entity, id)
        ads.save(ns, rect);
        assertEquals(1, ads.getCount(ns));
        final Rectangle rectLoaded = ads.get(ns, Rectangle.class, rect.getId());
        assertEquals(rect.getId(), rectLoaded.getId());
        assertEquals(rect.getArea(), rectLoaded.getArea(), 0);
    }

    @Test
    public void testFind() throws Exception {
        final String ns = "hotels";
        Rectangle rect = new Rectangle(10, 10);
        rect.setId("1");

        db.getCollection(ns).remove(new BasicDBObject());

        //test delete(entity, id)
        ads.save(ns, rect);
        assertEquals(1, ads.getCount(ns));
        Rectangle rectLoaded = ads.find(ns, Rectangle.class).get();
        assertEquals(rect.getId(), rectLoaded.getId());
        assertEquals(rect.getArea(), rectLoaded.getArea(), 0);

        rect = new Rectangle(2, 1);
        rect.setId("2");
        ads.save(rect); //saved to default collection name (kind)
        assertEquals(1, ads.getCount(rect));

        rect.setId("3");
        ads.save(rect); //saved to default collection name (kind)
        assertEquals(2, ads.getCount(rect));

        rect = new Rectangle(4, 3);
        rect.setId("3");
        ads.save(ns, rect);
        assertEquals(2, ads.getCount(ns));
        final List<Rectangle> rects = ads.find(ns, Rectangle.class).asList();

        rectLoaded = rects.get(1);
        assertEquals(rect.getId(), rectLoaded.getId());
        assertEquals(rect.getArea(), rectLoaded.getArea(), 0);

        ads.find(ns, Rectangle.class, "_id !=", "-1", 1, 1).get();
    }
}
