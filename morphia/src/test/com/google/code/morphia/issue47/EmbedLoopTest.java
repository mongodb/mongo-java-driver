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
package com.google.code.morphia.issue47;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class EmbedLoopTest extends TestBase {

    @Entity
    static class A extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Embedded
        private B b;
    }

    @Embedded
    static class B extends TestEntity {
        private static final long serialVersionUID = 1L;
        private final String someProperty = "someThing";

        // produces stackoverflow, might be detectable?
        // @Reference this would be right way to do it.

        @Embedded
        private A a;
    }

    @Test
    @Ignore
    public void testCircularRefs() throws Exception {

        morphia.map(A.class);

        A a = new A();
        a.b = new B();
        a.b.a = a;

        Assert.assertSame(a, a.b.a);

        this.ds.save(a);
        a = this.ds.find(A.class, "_id", a.getId()).get();
        Assert.assertSame(a, a.b.a);
    }
}