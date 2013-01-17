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

package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author josephpachod
 */
@SuppressWarnings("unused")
public class ReferencesInEmbeddedTest extends TestBase {
    @Entity
    private static class Container extends TestEntity {
        private static final long serialVersionUID = 1L;
        String name;
        @Embedded
        private EmbedContainingReference embed;
    }

    private static class EmbedContainingReference {
        String name;
        @Reference
        protected ReferencedEntity ref;

        @Reference(lazy = true)
        protected ReferencedEntity lazyRef;
    }

    @Entity
    public static class ReferencedEntity extends TestEntity {
        private static final long serialVersionUID = 1L;
        String foo;
    }

    @Test
    public void testMapping() throws Exception {
        morphia.map(Container.class);
        morphia.map(ReferencedEntity.class);
    }

    @Test
    public void testNonLazyReferencesInEmbebbed() throws Exception {
        final Container container = new Container();
        container.name = "nonLazy";
        ds.save(container);
        final ReferencedEntity referencedEntity = new ReferencedEntity();
        ds.save(referencedEntity);

        container.embed = new EmbedContainingReference();
        container.embed.ref = referencedEntity;
        ds.save(container);

        final Container reloadedContainer = ds.get(container);
        Assert.assertNotNull(reloadedContainer);
    }

    @Test
    public void testLazyReferencesInEmbebbed() throws Exception {
        final Container container = new Container();
        container.name = "lazy";
        ds.save(container);
        final ReferencedEntity referencedEntity = new ReferencedEntity();
        ds.save(referencedEntity);

        container.embed = new EmbedContainingReference();
        container.embed.lazyRef = referencedEntity;
        ds.save(container);

        final Container reloadedContainer = ds.get(container);
        Assert.assertNotNull(reloadedContainer);
    }
}
