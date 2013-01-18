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

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author josephpachod
 */
@SuppressWarnings("unused")
public class LazyInEmbeddedTest extends TestBase {
    public enum SomeEnum {
        B, A
    }

    @Entity
    public static class ContainerWithRefInField extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Embedded
        private EmbedWithRef embedWithRef;
    }

    @Entity
    public static class ContainerWithRefList extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Embedded
        private final List<EmbedWithRef> embedWithRef = new ArrayList<EmbedWithRef>();
    }

    @Entity
    public static class OtherEntity extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Property(value = "some")
        private SomeEnum someEnum;

        @SuppressWarnings("unused")
        protected OtherEntity() {
        }

        public OtherEntity(final SomeEnum someEnum) {
            this.someEnum = someEnum;

        }
    }

    @Entity
    public static class OtherEntityChild extends OtherEntity {
        private static final long serialVersionUID = 1L;

        public OtherEntityChild() {
            super(SomeEnum.A);
        }

        @Property
        private String name;
    }

    public static class EmbedWithRef {

        @Reference(lazy = true)
        private OtherEntity otherEntity;
    }

    @Test
    public void testLoadingOfRefInField() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        morphia.map(ContainerWithRefInField.class);
        morphia.map(OtherEntity.class);

        OtherEntity otherEntity = new OtherEntity();
        ContainerWithRefInField containerWithRefInField = new ContainerWithRefInField();

        ds.save(otherEntity, containerWithRefInField);

        otherEntity = ds.get(otherEntity);
        containerWithRefInField = ds.get(containerWithRefInField);
        Assert.assertNotNull(otherEntity);
        Assert.assertNotNull(containerWithRefInField);

        final EmbedWithRef embedWithRef = new EmbedWithRef();
        embedWithRef.otherEntity = otherEntity;
        containerWithRefInField.embedWithRef = embedWithRef;

        ds.save(containerWithRefInField);

        containerWithRefInField = ds.get(containerWithRefInField);
        Assert.assertNotNull(containerWithRefInField);

    }

    @Test
    public void testLoadingOfRefThroughInheritanceInField() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        morphia.map(ContainerWithRefInField.class);
        morphia.map(OtherEntityChild.class);

        OtherEntityChild otherEntity = new OtherEntityChild();
        ContainerWithRefInField containerWithRefInField = new ContainerWithRefInField();

        ds.save(otherEntity, containerWithRefInField);

        otherEntity = ds.get(otherEntity);
        final ContainerWithRefInField reload = ds.get(containerWithRefInField);
        Assert.assertNotNull(otherEntity);
        Assert.assertNotNull(reload);

        final EmbedWithRef embedWithRef = new EmbedWithRef();
        embedWithRef.otherEntity = otherEntity;
        reload.embedWithRef = embedWithRef;

        ds.save(reload);
        ds.get(reload);
        containerWithRefInField = ds.get(containerWithRefInField);
        Assert.assertNotNull(containerWithRefInField);

    }

    @Test
    public void testLoadingOfRefInList() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        morphia.map(ContainerWithRefList.class);
        morphia.map(OtherEntity.class);

        OtherEntity otherEntity = new OtherEntity();
        ContainerWithRefList containerWithRefInList = new ContainerWithRefList();

        ds.save(otherEntity, containerWithRefInList);

        otherEntity = ds.get(otherEntity);
        containerWithRefInList = ds.get(containerWithRefInList);
        Assert.assertNotNull(otherEntity);
        Assert.assertNotNull(containerWithRefInList);

        final EmbedWithRef embedWithRef = new EmbedWithRef();
        embedWithRef.otherEntity = otherEntity;
        containerWithRefInList.embedWithRef.add(embedWithRef);

        ds.save(otherEntity, containerWithRefInList);

        containerWithRefInList = ds.get(containerWithRefInList);
        Assert.assertNotNull(containerWithRefInList);

        final Query<ContainerWithRefList> createQuery = ds.createQuery(ContainerWithRefList.class);
        containerWithRefInList = createQuery.get();
        Assert.assertNotNull(containerWithRefInList);

    }

    @Test
    public void testLoadingOfRefThroughInheritanceInList() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        morphia.map(ContainerWithRefList.class);
        morphia.map(OtherEntityChild.class);

        OtherEntityChild otherEntity = new OtherEntityChild();
        ContainerWithRefList containerWithRefInList = new ContainerWithRefList();

        ds.save(otherEntity, containerWithRefInList);

        otherEntity = ds.get(otherEntity);
        final ContainerWithRefList reload = ds.get(containerWithRefInList);
        Assert.assertNotNull(otherEntity);
        Assert.assertNotNull(reload);

        final EmbedWithRef embedWithRef = new EmbedWithRef();
        embedWithRef.otherEntity = otherEntity;
        reload.embedWithRef.add(embedWithRef);

        ds.save(otherEntity, reload);

        ds.get(reload);

        containerWithRefInList = ds.get(reload);
        Assert.assertNotNull(containerWithRefInList);
        final Query<ContainerWithRefList> createQuery = ds.createQuery(ContainerWithRefList.class);
        containerWithRefInList = createQuery.get();
        Assert.assertNotNull(containerWithRefInList);

    }
}
