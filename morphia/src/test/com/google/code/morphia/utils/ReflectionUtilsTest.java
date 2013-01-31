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


import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Index;
import com.google.code.morphia.annotations.Indexes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author Scott Hernandez
 */
public class ReflectionUtilsTest extends TestBase {

    /**
     * Test method for {@link com.google.code.morphia.utils.ReflectionUtils#implementsInterface(java.lang.Class,
     * java.lang.Class)} .
     */
    @Test
    public void testImplementsInterface() {
        Assert.assertTrue(ReflectionUtils.implementsInterface(ArrayList.class, List.class));
        Assert.assertTrue(ReflectionUtils.implementsInterface(ArrayList.class, Collection.class));
        Assert.assertFalse(ReflectionUtils.implementsInterface(Set.class, List.class));
    }

    @Test
    public void testInheritedClassAnnotations() {
        final List<Indexes> annotations = ReflectionUtils.getAnnotations(Foobie.class, Indexes.class);
        Assert.assertEquals(2, annotations.size());
        Assert.assertTrue(ReflectionUtils.getAnnotation(Foobie.class, Indexes.class) instanceof Indexes);
    }

    @Indexes(@Index("id"))
    private static class Foo {
        @Id
        private int id;
    }

    @Indexes(@Index("test"))
    private static class Foobie extends Foo {
        private String test;
    }
}
