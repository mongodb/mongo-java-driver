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

package com.google.code.morphia.issue155;

import com.google.code.morphia.TestBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author josephpachod
 */
public class EnumBehindAnInterfaceTest extends TestBase {
    @Test
    @Ignore("does not work since the EnumConverter stores as a single string value -- no type info")
    public void testEnumBehindAnInterfacePersistence() throws Exception {
        morphia.map(ContainerEntity.class);
        ContainerEntity n = new ContainerEntity();
        ds.save(n);
        n = ds.get(n);
        Assert.assertSame(EnumBehindAnInterface.A, n.foo);
    }
}
