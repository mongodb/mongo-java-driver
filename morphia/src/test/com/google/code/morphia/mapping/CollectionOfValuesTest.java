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

import com.google.code.morphia.Key;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class CollectionOfValuesTest extends TestBase {

    public static class ContainsListOfList {
        @Id
        private ObjectId id;
        @Embedded
        private List<List<String>> listOfList;
    }

    public static class ContainsTwoDimensionalArray {
        @Id
        private String id;
        @Embedded
        private byte[][] twoDimArray;
    }

    @Test
    @Ignore("Not yet implemented")
    public void testTwoDimensionalArrayMapping() throws Exception {
        morphia.map(ContainsTwoDimensionalArray.class);
        final ContainsTwoDimensionalArray entity = new ContainsTwoDimensionalArray();
        final byte[][] test2DimBa = new byte[][]{"Joseph".getBytes(), "uwe".getBytes()};
        entity.twoDimArray = test2DimBa;
        final Key<ContainsTwoDimensionalArray> savedKey = ds.save(entity);
        final ContainsTwoDimensionalArray loaded = ds.get(ContainsTwoDimensionalArray.class, savedKey.getId());
        Assert.assertNotNull(loaded.twoDimArray);
        Assert.assertArrayEquals(test2DimBa, loaded.twoDimArray);
        Assert.assertNotNull(loaded.id);
    }

    @Test
    public void testListOfListMapping() throws Exception {
        morphia.map(ContainsListOfList.class);
        ds.delete(ds.find(ContainsListOfList.class));
        final ContainsListOfList entity = new ContainsListOfList();
        final ArrayList<List<String>> testList = new ArrayList<List<String>>();
        final ArrayList<String> element1 = new ArrayList<String>();
        element1.add("element1");
        testList.add(element1);

        final List<String> element2 = new ArrayList<String>();
        element2.add("element2");
        testList.add(element2);

        entity.listOfList = testList;
        ds.save(entity);
        final ContainsListOfList loaded = ds.get(entity);

        Assert.assertNotNull(loaded.listOfList);

        Assert.assertEquals(testList, loaded.listOfList);
        final List<String> loadedElement1 = loaded.listOfList.get(0);
        Assert.assertEquals(element1, loadedElement1);
        Assert.assertEquals(element1.get(0), loadedElement1.get(0));
        Assert.assertNotNull(loaded.id);
    }

}
