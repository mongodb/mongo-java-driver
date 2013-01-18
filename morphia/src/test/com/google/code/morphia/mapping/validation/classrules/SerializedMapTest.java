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
package com.google.code.morphia.mapping.validation.classrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class SerializedMapTest extends TestBase {

    static class Map1 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Serialized(disableCompression = false)
        private final Map<Integer, Foo> shouldBeOk = new HashMap<Integer, Foo>();

    }

    static class Map2 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Serialized(disableCompression = true)
        private final Map<Integer, Foo> shouldBeOk = new HashMap<Integer, Foo>();

    }

    static class Foo implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String id;

        public Foo(final String id) {
            this.id = id;
        }
    }

    @Test
    public void testSerialization() throws Exception {
        Map1 map1 = new Map1();
        map1.shouldBeOk.put(3, new Foo("peter"));
        map1.shouldBeOk.put(27, new Foo("paul"));

        ds.save(map1);
        map1 = ds.get(map1);

        Assert.assertEquals("peter", map1.shouldBeOk.get(3).id);
        Assert.assertEquals("paul", map1.shouldBeOk.get(27).id);

    }

    @Test
    public void testSerialization2() throws Exception {
        Map2 map2 = new Map2();
        map2.shouldBeOk.put(3, new Foo("peter"));
        map2.shouldBeOk.put(27, new Foo("paul"));

        ds.save(map2);
        map2 = ds.get(map2);

        Assert.assertEquals("peter", map2.shouldBeOk.get(3).id);
        Assert.assertEquals("paul", map2.shouldBeOk.get(27).id);

    }
}
