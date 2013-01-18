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

/**
 *
 */
package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scott hernandez
 */
@SuppressWarnings("unused")
public class MapImplTest extends TestBase {

    private static class ContainsMapOfEmbeddedInterfaces {
        @Id
        private ObjectId id;
        @Embedded
        private final Map<String, Serializable> values = new HashMap<String, Serializable>();
    }

    private static class ContainsMapOfEmbeddedGoos {
        @Id
        private ObjectId id;
        @Embedded
        private final Map<String, Goo> values = new HashMap<String, Goo>();
    }

    private static class Goo implements Serializable {
        static final long serialVersionUID = 1L;
        @Id
        private ObjectId id;
        private String name;

        Goo() {
        }

        Goo(final String n) {
            name = n;
        }
    }

    private static class E {
        @Id
        private ObjectId id;

        @Embedded
        private final MyMap mymap = new MyMap();
    }

    private static class MyMap extends HashMap<String, String> {
        private static final long serialVersionUID = 1L;
    }

    @Test
    public void testMapping() throws Exception {
        E e = new E();
        e.mymap.put("1", "a");
        e.mymap.put("2", "b");

        ds.save(e);

        e = ds.get(e);
        Assert.assertEquals("a", e.mymap.get("1"));
        Assert.assertEquals("b", e.mymap.get("2"));
    }

    @Test
    public void testEmbeddedMap() throws Exception {
        morphia.map(ContainsMapOfEmbeddedGoos.class).map(ContainsMapOfEmbeddedInterfaces.class);
        final Goo g1 = new Goo("Scott");
        final ContainsMapOfEmbeddedGoos cmoeg = new ContainsMapOfEmbeddedGoos();
        cmoeg.values.put("first", g1);
        ds.save(cmoeg);
        //check className in the map values.

        final BasicDBObject goo = (BasicDBObject) ((BasicDBObject) ds.getCollection(ContainsMapOfEmbeddedGoos.class)
                .findOne().get("values")).get("first");
        final boolean hasF = goo.containsField(Mapper.CLASS_NAME_FIELDNAME);
        assertTrue(!hasF);
    }

    @Test
    public void testEmbeddedMapWithValueInterface() throws Exception {
        morphia.map(ContainsMapOfEmbeddedGoos.class).map(ContainsMapOfEmbeddedInterfaces.class);
        final Goo g1 = new Goo("Scott");

        final ContainsMapOfEmbeddedInterfaces cmoei = new ContainsMapOfEmbeddedInterfaces();
        cmoei.values.put("first", g1);
        ds.save(cmoei);
        //check className in the map values.
        final BasicDBObject goo = (BasicDBObject) ((BasicDBObject) ds.getCollection(
                ContainsMapOfEmbeddedInterfaces.class).findOne().get("values")).get("first");
        final boolean hasF = goo.containsField(Mapper.CLASS_NAME_FIELDNAME);
        assertTrue(hasF);
    }

    @Test
    public void testEmbeddedMapUpdateOperationsOnInterfaceValue() throws Exception {
        morphia.map(ContainsMapOfEmbeddedGoos.class).map(ContainsMapOfEmbeddedInterfaces.class);
        final Goo g1 = new Goo("Scott");
        final Goo g2 = new Goo("Ralph");

        final ContainsMapOfEmbeddedInterfaces cmoei = new ContainsMapOfEmbeddedInterfaces();
        cmoei.values.put("first", g1);
        ds.save(cmoei);
        ds.update(cmoei, ds.createUpdateOperations(ContainsMapOfEmbeddedInterfaces.class).set("values.second", g2));
        //check className in the map values.
        final BasicDBObject goo = (BasicDBObject) ((BasicDBObject) ds.getCollection(
                ContainsMapOfEmbeddedInterfaces.class).findOne().get("values")).get("second");
        final boolean hasF = goo.containsField(Mapper.CLASS_NAME_FIELDNAME);
        assertTrue("className should be here.", hasF);
    }

    @Test //@Ignore("waiting on issue 184")
    public void testEmbeddedMapUpdateOperations() throws Exception {
        morphia.map(ContainsMapOfEmbeddedGoos.class).map(ContainsMapOfEmbeddedInterfaces.class);
        final Goo g1 = new Goo("Scott");
        final Goo g2 = new Goo("Ralph");

        final ContainsMapOfEmbeddedGoos cmoeg = new ContainsMapOfEmbeddedGoos();
        cmoeg.values.put("first", g1);
        ds.save(cmoeg);
        ds.update(cmoeg, ds.createUpdateOperations(ContainsMapOfEmbeddedGoos.class).set("values.second", g2));
        //check className in the map values.

        final BasicDBObject goo = (BasicDBObject) ((BasicDBObject) ds.getCollection(
                ContainsMapOfEmbeddedGoos.class).findOne().get("values")).get("second");
        final boolean hasF = goo.containsField(Mapper.CLASS_NAME_FIELDNAME);
        assertTrue("className should not be here.", !hasF);
    }
}
