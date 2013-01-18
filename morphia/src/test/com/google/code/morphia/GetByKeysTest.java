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
package com.google.code.morphia;

import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class GetByKeysTest extends TestBase {
    @Test
    public final void testGetByKeys() {
        final A a1 = new A();
        final A a2 = new A();

        final Iterable<Key<A>> keys = ds.save(a1, a2);

        final List<A> reloaded = ds.getByKeys(keys);

        final Iterator<A> i = reloaded.iterator();
        Assert.assertNotNull(i.next());
        Assert.assertNotNull(i.next());
        Assert.assertFalse(i.hasNext());
    }

    private static class A extends TestEntity {
        private static final long serialVersionUID = 1L;
        private final String foo = "bar";
    }

}
