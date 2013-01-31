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
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class MapNotSerializableTest extends TestBase {
    @SuppressWarnings("UnusedDeclaration")
    static class Map1 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Serialized
        private final Map<Integer, String> shouldBeOk = new HashMap<Integer, String>();

    }

    @SuppressWarnings("UnusedDeclaration")
    static class Map2 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Reference
        private final Map<Integer, E1> shouldBeOk = new HashMap<Integer, E1>();

    }

    @SuppressWarnings("UnusedDeclaration")
    static class Map3 extends TestEntity {
        @SuppressWarnings("UnusedDeclaration")
        private static final long serialVersionUID = 1L;
        @Embedded
        private final Map<E2, Integer> shouldBeOk = new HashMap<E2, Integer>();

    }

    static class E1 {
    }

    static class E2 {
    }

    @Test
    public void testCheck() {
        morphia.map(Map1.class);

        new AssertedFailure() {
            public void thisMustFail() {
                morphia.map(Map2.class);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                morphia.map(Map3.class);
            }
        };
    }

}
