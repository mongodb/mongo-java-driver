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

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ConcreteClassEmbeddedOverrrideTest extends TestBase {

    public static class E {
        @Id
        private ObjectId id;

        @Embedded
        private final A a1 = new A();

        @Embedded(concreteClass = B.class)
        private final A a2 = new A();
    }

    public static class A {
        String s = "A";
    }

    public static class B extends A {
        public B() {
            s = "B";
        }
    }

    @Test
    public void test() throws Exception {
        final E e1 = new E();
        Assert.assertEquals("A", e1.a1.s);
        Assert.assertEquals("A", e1.a2.s);

        ds.save(e1);

        final E e2 = ds.get(e1);

        Assert.assertEquals("A", e2.a1.s);
        Assert.assertEquals("A", e2.a2.s);
        Assert.assertEquals(B.class, e2.a2.getClass());
        Assert.assertEquals(A.class, e2.a1.getClass());

    }
}
