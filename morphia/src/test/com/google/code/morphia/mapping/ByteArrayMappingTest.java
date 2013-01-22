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
package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ByteArrayMappingTest extends TestBase {
    private static class ContainsByteArray {
        @Id
        private ObjectId id;
        private Byte[] ba;
    }


    @Test
    public void testCharMapping() throws Exception {
        morphia.map(ContainsByteArray.class);
        final ContainsByteArray entity = new ContainsByteArray();
        final Byte[] test = new Byte[]{6, 9, 1, -122};
        entity.ba = test;
        ds.save(entity);
        final ContainsByteArray loaded = ds.get(entity);

        for (int i = 0; i < test.length; i++) {
            final Byte c = test[i];
            Assert.assertEquals(c, entity.ba[i]);
        }
        Assert.assertNotNull(loaded.id);
    }

}
