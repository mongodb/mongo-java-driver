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

/**
 *
 */
package com.google.code.morphia.mapping;

import com.google.code.morphia.AdvancedDatastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

/**
 * @author scott hernandez
 */
@SuppressWarnings("unused")
public class CompoundIdTest extends TestBase {

    @Embedded
    private static class CId implements Serializable {
        static final long serialVersionUID = 1L;
        private final ObjectId id = new ObjectId();
        private String name;

        CId() {
        }

        CId(final String n) {
            name = n;
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof CId)) {
                return false;
            }
            final CId other = ((CId) obj);
            return other.id.equals(id) && other.name.equals(name);
        }

        @Override
        public int hashCode() {
            int result = id != null ? id.hashCode() : 0;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }

    private static class E {
        @Id
        private CId id;
        private String e;
    }

    @Test
    public void testMapping() throws Exception {
        E e = new E();
        e.id = new CId("test");

        final Key<E> key = ds.save(e);
        e = ds.get(e);
        Assert.assertEquals("test", e.id.name);
        Assert.assertNotNull(e.id.id);
    }

    @Test
    public void testDelete() throws Exception {
        final E e = new E();
        e.id = new CId("test");

        final Key<E> key = ds.save(e);
        ds.delete(E.class, e.id);
    }

    @Test
    public void testOtherDelete() throws Exception {
        final E e = new E();
        e.id = new CId("test");

        final Key<E> key = ds.save(e);
        ((AdvancedDatastore) ds).delete(ds.getCollection(E.class).getName(), E.class, e.id);
    }

}
