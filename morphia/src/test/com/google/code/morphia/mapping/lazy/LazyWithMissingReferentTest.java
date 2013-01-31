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

package com.google.code.morphia.mapping.lazy;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Iterator;

@SuppressWarnings("unused")
public class LazyWithMissingReferentTest extends TestBase {

    static class E {
        @Id
        private final ObjectId id = new ObjectId();
        @Reference
        private E2 e2;
    }

    static class ELazy {
        @Id
        private final ObjectId id = new ObjectId();
        @Reference(lazy = true)
        private E2 e2;
    }

    static class ELazyIgnoreMissing {
        @Id
        private final ObjectId id = new ObjectId();
        @Reference(lazy = true, ignoreMissing = true)
        private E2 e2;
    }

    static class E2 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Id
        private final ObjectId id = new ObjectId();
        private final String foo = "bar";

        void foo() {
        }

    }

    @Test
    public void testMissingRef() throws Exception {
        final E e = new E();
        final E2 e2 = new E2();
        e.e2 = e2;

        ds.save(e); // does not fail due to preinited Ids

        new AssertedFailure(MappingException.class) {
            @Override
            protected void thisMustFail() {
                ds.createQuery(E.class).asList();
            }
        };
    }

    @Test
    public void testMissingRefLazy() throws Exception {
        final ELazy e = new ELazy();
        final E2 e2 = new E2();
        e.e2 = e2;

        ds.save(e); // does not fail due to preinited Ids

        new AssertedFailure(MappingException.class) {
            @Override
            protected void thisMustFail() {
                ds.createQuery(ELazy.class).asList();
            }
        };
    }

    @Test
    public void testMissingRefLazyIgnoreMissing() throws Exception {
        final ELazyIgnoreMissing e = new ELazyIgnoreMissing();
        final E2 e2 = new E2();
        e.e2 = e2;

        ds.save(e); // does not fail due to preinited Ids
        final Iterator<ELazyIgnoreMissing> i = ds.createQuery(ELazyIgnoreMissing.class).iterator();
        final ELazyIgnoreMissing x = i.next();

        new AssertedFailure() {
            @Override
            protected void thisMustFail() {
                // reference must be resolved for this
                x.e2.foo();
            }
        };
    }
}
