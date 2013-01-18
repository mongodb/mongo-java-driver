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
package com.google.code.morphia.utils;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.TestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author ScottHernandez
 */
public class LongIdEntityTest extends TestBase {
    static class MyEntity extends LongIdEntity {
        protected MyEntity() {
            super(null);
        }

        public MyEntity(final Datastore ds) {
            super(ds);
        }
    }

    @Test
    public void testMonoIncreasingId() throws Exception {
        MyEntity ent = new MyEntity(ds);
        ds.save(ent);
        assertEquals(1L, ent.myLongId, 0);
        ent = new MyEntity(ds);
        ds.save(ent);
        assertEquals(2L, ent.myLongId, 0);
    }

}
