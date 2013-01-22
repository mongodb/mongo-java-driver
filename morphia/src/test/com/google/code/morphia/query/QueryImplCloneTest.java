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
package com.google.code.morphia.query;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("rawtypes")
public class QueryImplCloneTest extends TestBase {
    private static final List<String> ALLOWED_CHANGING_FIELDS = Arrays.asList("cache", "query");

    private <T> boolean sameState(final Query<T> q1, final Query<T> q2) throws IllegalAccessException {
        return sameState(q1.getClass(), q1, q2);
    }

    private <T> boolean sameState(final Class c, final Query<T> q1, final Query<T> q2) throws IllegalAccessException {
        final Field[] fields = c.getDeclaredFields();
        for (final Field f : fields) {
            f.setAccessible(true);

            final Object v1 = f.get(q1);
            final Object v2 = f.get(q2);

            if (v1 == null && v2 == null) {
                continue;
            }

            if (v1 != null && v1.equals(v2)) {
                continue;
            }

            if (!ALLOWED_CHANGING_FIELDS.contains(f.getName())) {
                throw new RuntimeException(f.getName() + " v1=" + v1 + " v2=" + v2);
            }
        }

        final Class superclass = c.getSuperclass();
        return (superclass == null || sameState(superclass, q1, q2));
    }

    @SuppressWarnings("unused")
    static class E1 {
        @Id
        private ObjectId id;

        private String a;
        private String b;
        private int i;
        private final E2 e2 = new E2();
    }

    @SuppressWarnings("unused")
    static class E2 {
        private String foo;
    }

    @Test
    public void testQueryClone() throws Exception {
        final Query<E1> q = ds.createQuery(E1.class).field("i").equal(5).limit(5).
                                                                                 filter("a", "value_a")
                              .filter("b", "value_b")
                              .offset(5).batchSize(10).disableCursorTimeout().hintIndex("a")
                              .order("a");
        q.disableValidation().filter("foo", "bar");
        Assert.assertTrue(sameState(q, q.clone()));
    }
}
