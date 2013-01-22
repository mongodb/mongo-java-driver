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

import com.google.code.morphia.Key;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.lazy.proxy.LazyReferenceFetchingException;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedEntityReference;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;

public class LazySingleReferenceTest extends ProxyTestBase {
//    @Test(expected = LazyReferenceFetchingException.class)
    public final void testCreateProxy() {

        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        LazySingleReferenceRootEntity root = new LazySingleReferenceRootEntity();
        final LazySingleReferenceReferencedEntity referenced = new LazySingleReferenceReferencedEntity();

        root.r = referenced;
        root.r.setFoo("bar");

        ds.save(referenced);
        ds.save(root);

        root = ds.get(root);

        assertNotFetched(root.r);
        Assert.assertEquals("bar", root.r.getFoo());
        assertFetched(root.r);
        Assert.assertEquals("bar", root.r.getFoo());

        // now remove it from DB
        ds.delete(root.r);

        root = deserialize(root);
        assertNotFetched(root.r);

        try {
            // must fail
            root.r.getFoo();
            Assert.fail("Expected Exception did not happen");
        } catch (LazyReferenceFetchingException expected) {
            // fine
        }

    }

    @Test
    public final void testShortcutInterface() {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        LazySingleReferenceRootEntity root = new LazySingleReferenceRootEntity();
        final LazySingleReferenceReferencedEntity reference = new LazySingleReferenceReferencedEntity();

        root.r = reference;
        reference.setFoo("bar");

        final Key<LazySingleReferenceReferencedEntity> k = ds.save(reference);
        final String keyAsString = k.getId().toString();
        ds.save(root);

        root = ds.get(root);

        LazySingleReferenceReferencedEntity p = root.r;

        assertIsProxy(p);
        assertNotFetched(p);
        Assert.assertEquals(keyAsString, ((ProxiedEntityReference) p).__getKey().getId().toString());
        // still unfetched?
        assertNotFetched(p);
        p.getFoo();
        // should be fetched now.
        assertFetched(p);

        root = deserialize(root);
        p = root.r;
        assertNotFetched(p);
        p.getFoo();
        // should be fetched now.
        assertFetched(p);

        root = ds.get(root);
        p = root.r;
        assertNotFetched(p);
        ds.save(root);
        assertNotFetched(p);
    }

    @Test
    @Ignore
    // FIXME us
    public final void testSameProxy() {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        LazySingleReferenceRootEntity root = new LazySingleReferenceRootEntity();
        final LazySingleReferenceReferencedEntity reference = new LazySingleReferenceReferencedEntity();

        root.r = reference;
        root.secondReference = reference;
        reference.setFoo("bar");

        ds.save(reference);
        ds.save(root);

        root = ds.get(root);
        Assert.assertSame(root.r, root.secondReference);
    }

    @Test
    public final void testSerialization() {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        LazySingleReferenceRootEntity e1 = new LazySingleReferenceRootEntity();
        final LazySingleReferenceReferencedEntity e2 = new LazySingleReferenceReferencedEntity();

        e1.r = e2;
        e2.setFoo("bar");

        ds.save(e2);
        ds.save(e1);

        e1 = deserialize(ds.get(e1));

        assertNotFetched(e1.r);
        Assert.assertEquals("bar", e1.r.getFoo());
        assertFetched(e1.r);

        e1 = deserialize(e1);
        assertNotFetched(e1.r);
        Assert.assertEquals("bar", e1.r.getFoo());
        assertFetched(e1.r);

    }

    @Test
    public final void testGetKeyWithoutFetching() {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        LazySingleReferenceRootEntity root = new LazySingleReferenceRootEntity();
        final LazySingleReferenceReferencedEntity reference = new LazySingleReferenceReferencedEntity();

        root.r = reference;
        reference.setFoo("bar");

        final Key<LazySingleReferenceReferencedEntity> k = ds.save(reference);
        final String keyAsString = k.getId().toString();
        ds.save(root);

        root = ds.get(root);

        final LazySingleReferenceReferencedEntity p = root.r;

        assertIsProxy(p);
        assertNotFetched(p);
        Assert.assertEquals(keyAsString, ds.getKey(p).getId().toString());
        // still unfetched?
        assertNotFetched(p);
        p.getFoo();
        // should be fetched now.
        assertFetched(p);

    }

    public static class LazySingleReferenceRootEntity extends TestEntity implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        @Reference(lazy = true)
        private LazySingleReferenceReferencedEntity r;
        @Reference(lazy = true)
        private LazySingleReferenceReferencedEntity secondReference;

    }

    public static class LazySingleReferenceReferencedEntity extends TestEntity implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private String foo;

        public void setFoo(final String string) {
            foo = string;
        }

        public String getFoo() {
            return foo;
        }
    }

}
