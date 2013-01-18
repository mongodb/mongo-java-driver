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
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;

/**
 * @author scotthernandez
 */
public class ListOfStringArrayMappingTest extends TestBase {
    private static class ContainsListStringArray {
        @Id
        private ObjectId id;
        private final ArrayList<String[]> listOfStrings = new ArrayList<String[]>();
    }

    @Test
    @Ignore
    //Add back when we figure out why we get this strange error :java.lang.ClassCastException: java.lang.String
    // cannot be cast to [Ljava.lang.String;
    public void testMapping() throws Exception {
        morphia.map(ContainsListStringArray.class);
        final ContainsListStringArray ent = new ContainsListStringArray();
        ent.listOfStrings.add(new String[]{"a", "b"});
        ds.save(ent);
        final ContainsListStringArray loaded = ds.get(ent);
        Assert.assertNotNull(loaded.id);
        final String[] arr = loaded.listOfStrings.get(0);
        final String a = arr[0];
        final String b = arr[1];
        Assert.assertEquals("a", a);
        Assert.assertEquals("b", b);

    }


}
