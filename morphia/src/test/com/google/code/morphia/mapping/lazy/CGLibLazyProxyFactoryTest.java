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

package com.google.code.morphia.mapping.lazy;

import com.google.code.morphia.Key;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Test;

public class CGLibLazyProxyFactoryTest extends ProxyTestBase {
    @Test
    public final void testCreateProxy() {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        final E e = new E();
        e.setFoo("bar");
        final Key<E> key = ds.save(e);
        E eProxy = new CGLibLazyProxyFactory().createProxy(E.class, key,
                                                          new DefaultDatastoreProvider());

        assertNotFetched(eProxy);
        Assert.assertEquals("bar", eProxy.getFoo());
        assertFetched(eProxy);

        eProxy = deserialize(eProxy);
        assertNotFetched(eProxy);
        Assert.assertEquals("bar", eProxy.getFoo());
        assertFetched(eProxy);

    }

    public static class E extends TestEntity {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private String foo;

        public void setFoo(final String string) {
            foo = string;
        }

        public String getFoo() {
            return foo;
        }
    }

}
