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

package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.ConstructorArgs;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Hernandez
 */
public class ConstructorArgsTest extends TestBase {

    @SuppressWarnings("unused")
    private static class Normal {
        @Id
        private final ObjectId id = new ObjectId();
        @ConstructorArgs("_id")
        private final ArgsConstructor ac = new ArgsConstructor(new ObjectId());
    }

    private static final class ArgsConstructor {
        @Id
        private final ObjectId id;

        private ArgsConstructor(final ObjectId id) {
            this.id = id;
        }
    }

    @Test
    public void testBasic() throws Exception {
        Normal n = new Normal();
        final ObjectId acId = n.ac.id;

        ds.save(n);
        n = ds.find(Normal.class).get();
        Assert.assertNotNull(n);
        Assert.assertNotNull(n.ac);
        Assert.assertEquals(acId, n.ac.id);
    }
}