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
package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author scotthernandez
 */
public class ListOfDoubleArrayMappingTest extends TestBase {
    private static class ContainsListDoubleArray {
        @Id
        private ObjectId id;
        private final List<Double[]> points = new ArrayList<Double[]>();
    }

    // TODO: Fails with Java 7
    @Test
    @Ignore("Ignore until we can get this working with Java 7")
    public void testMapping() throws Exception {
        morphia.map(ContainsListDoubleArray.class);
        final ContainsListDoubleArray ent = new ContainsListDoubleArray();
        ent.points.add(new Double[]{1.1, 2.2});
        ds.save(ent);
        final ContainsListDoubleArray loaded = ds.get(ent);
        Assert.assertNotNull(loaded.id);
        //        Assert.assertEquals(1.1D, loaded.points.get(0)[0], 0);
        //        Assert.assertEquals(2.2D, loaded.points.get(0)[1], 0);
    }

}
