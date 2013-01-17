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

import com.google.code.morphia.Key;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.testutil.AssertedFailure;
import org.junit.Test;


public class MandatoryIdTest extends TestBase {

    @Entity
    public static class E {
        // not id here
        private final String foo = "bar";
    }

    @Test
    public final void testMissingId() {
        new AssertedFailure() {

            @Override
            protected void thisMustFail() throws Throwable {
                morphia.map(E.class);
            }
        };
    }

    @Test
    public final void testMissingIdNoImplicitMapCall() {
        final Key<E> save = ds.save(new E());

        new AssertedFailure() {
            @Override
            protected void thisMustFail() throws Throwable {
                final E byKey = ds.getByKey(E.class, save);
            }
        };
    }

}
