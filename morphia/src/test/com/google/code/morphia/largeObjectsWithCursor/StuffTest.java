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

package com.google.code.morphia.largeObjectsWithCursor;

import com.google.code.morphia.MappingTest.BaseEntity;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.query.Query;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Test from list, but doesn't seems to be a problem. Here as an example.
 */
public class StuffTest extends TestBase {
    private int documentsNb;

    @Entity
    static class E extends BaseEntity {
        private static final long serialVersionUID = 1L;
        protected final Integer index;
        protected final byte[] largeContent;

        public E() {
            index = null;
            largeContent = null;
        }

        private byte[] createLargeByteArray() {
            final int size = (int) (4000 + Math.random() * 100000);
            final byte[] arr = new byte[size];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = 'a';
            }
            return arr;
        }

        public E(final int i) {
            this.index = i;
            largeContent = createLargeByteArray();
        }

    }

    @Override
    @Before
    public void setUp() {
//		try {
//			this.mongo = new Mongo("stump");
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}

        super.setUp();
        morphia.map(E.class);
        documentsNb = 1000;
        for (int i = 0; i < documentsNb; i++) {
            ds.save(new E(i));
        }
    }

    @Test
    public void testWithManyElementsInCollection() throws Exception {
        Query<E> query = ds.createQuery(E.class);
        final long countAll = query.countAll();
        query = ds.createQuery(E.class);
        final List<E> list = query.asList();
        Assert.assertEquals(documentsNb, countAll);
        Assert.assertEquals(documentsNb, list.size());
    }
}