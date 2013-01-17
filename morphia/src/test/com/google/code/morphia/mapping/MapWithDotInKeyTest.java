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

/**
 * @author scott hernandez
 */
public class MapWithDotInKeyTest extends TestBase {

    @SuppressWarnings("unused")
    private static class Goo implements Serializable {
        static final long serialVersionUID = 1L;
        @Id
        ObjectId id = new ObjectId();
        String name;

        Goo() {
        }

        Goo(final String n) {
            name = n;
        }
    }

    private static class E {
        @SuppressWarnings("unused")
        @Id
        ObjectId id;

        @Embedded
        final
        MyMap mymap = new MyMap();
    }

    private static class MyMap extends BasicDBObject {
        private static final long serialVersionUID = 1L;
    }

    @Test
    public void testMapping() throws Exception {
        E e = new E();
        e.mymap.put("a.b", "a");
        e.mymap.put("c.e.g", "b");

        try {
            ds.save(e);
        } catch (Exception ex) {
            return;
        }

        Assert.assertFalse("Should have got rejection for dot in field names", true);
        e = ds.get(e);
        Assert.assertEquals("a", e.mymap.get("a.b"));
        Assert.assertEquals("b", e.mymap.get("c.e.g"));
    }
}
