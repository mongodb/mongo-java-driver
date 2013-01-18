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

package com.google.code.morphia.issue49;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

public class ReferenceArrayTest extends TestBase {

    @Test
    public final void testArrayPersistence() {
        A a = new A();
        final B b1 = new B();
        final B b2 = new B();

        a.bs[0] = b1;
        a.bs[1] = b2;

        ds.save(b2, b1, a);

        a = ds.get(a);
    }

    static class A extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Reference
        private final B[] bs = new B[2];
    }

    static class B extends TestEntity {
        private static final long serialVersionUID = 1L;
        private String foo;
    }

}
