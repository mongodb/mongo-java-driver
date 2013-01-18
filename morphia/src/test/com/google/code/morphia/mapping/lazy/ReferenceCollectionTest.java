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

package com.google.code.morphia.mapping.lazy;

import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ReferenceCollectionTest extends ProxyTestBase {
    @Test
    public final void testCreateProxy() {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        A a = new A();
        final B b1 = new B();
        final B b2 = new B();

        a.bs.add(b1);
        a.bs.add(b2);

        Collection<B> lazyBs = a.lazyBs;
        lazyBs.add(b1);
        lazyBs.add(b2);

        ds.save(b2, b1, a);

        a = ds.get(a);

        lazyBs = a.lazyBs;
        Assert.assertNotNull(lazyBs);
        assertNotFetched(lazyBs);

        Assert.assertNotNull(lazyBs.iterator().next());
        assertFetched(lazyBs);

        a = deserialize(a);

        lazyBs = a.lazyBs;
        Assert.assertNotNull(lazyBs);
        assertNotFetched(lazyBs);

        Assert.assertNotNull(lazyBs.iterator().next());
        assertFetched(lazyBs);

        a = deserialize(a);

        ds.save(a);
        lazyBs = a.lazyBs;
        assertNotFetched(lazyBs);
    }

    @Test
    public void testOrderingPreserved() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        final A a = new A();
        final B b1 = new B();
        b1.setFoo("b1");
        a.lazyBs.add(b1);

        final B b2 = new B();
        b2.setFoo("b2");
        a.lazyBs.add(b2);
        ds.save(b1);
        ds.save(b2);

        Assert.assertEquals("b1", a.lazyBs.iterator().next().foo);

        ds.save(a);

        A reloaded = ds.get(a);
        Assert.assertEquals("b1", reloaded.lazyBs.iterator().next().foo);
        Collections.swap((List<B>) reloaded.lazyBs, 0, 1);
        Assert.assertEquals("b2", reloaded.lazyBs.iterator().next().foo);

        ds.save(reloaded);

        reloaded = ds.get(reloaded);
        final Collection<B> lbs = reloaded.lazyBs;
        Assert.assertEquals(2, lbs.size());
        final Iterator<B> iterator = lbs.iterator();
        Assert.assertEquals("b2", iterator.next().foo);

    }

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    static class A extends TestEntity implements Serializable {
        private static final long serialVersionUID = 1L;

        @Reference(lazy = false)
        private final Collection<B> bs = new LinkedList<B>();

        @Reference(lazy = true)
        private final Collection<B> lazyBs = new LinkedList<B>();

    }

    static class B extends TestEntity implements Serializable {
        private static final long serialVersionUID = 1L;
        private String foo;

        public void setFoo(final String string) {
            this.foo = string;
        }

        @Override
        public String toString() {
            return super.toString() + " : id = " + getId() + ", foo = " + foo;
        }
    }

}
