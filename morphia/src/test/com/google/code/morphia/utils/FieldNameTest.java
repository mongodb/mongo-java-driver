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
package com.google.code.morphia.utils;

import com.google.code.morphia.testutil.AssertedFailure;
import org.junit.Test;

public class FieldNameTest {

    private String foo;
    private String bar;

    @Test
    public void testFieldNameOf() throws Exception {
        final String name = "foo";
        org.junit.Assert.assertEquals("foo", FieldName.of("foo"));
        org.junit.Assert.assertEquals("bar", FieldName.of("bar"));
        new AssertedFailure(FieldName.FieldNameNotFoundException.class) {

            @Override
            protected void thisMustFail() {
                FieldName.of("buh");
            }
        };
        org.junit.Assert.assertEquals("x", FieldName.of(E2.class, "x"));
        org.junit.Assert.assertEquals("y", FieldName.of(E2.class, "y"));
    }
}

class E1 {
    private final int x = 0;
}

class E2 extends E1 {
    private final int y = 0;
}
