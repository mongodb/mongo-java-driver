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
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author scotthernandez
 */
public class NestedMapOrCollTest extends TestBase {
    private static class HasMapOfMap {
        @Id
        private ObjectId id;

        @Embedded
        private final Map<String, Map<String, String>> mom = new HashMap<String, Map<String, String>>();
    }

    private static class HasMapOfList {
        @Id
        private ObjectId id;

        @Embedded
        private final Map<String, List<String>> mol = new HashMap<String, List<String>>();
    }

    private static class HasMapOfListOfMapMap {
        @Id
        private ObjectId id;

        @Embedded
        private final Map<String, List<NestedMapOrCollTest.HasMapOfMap>> mol = new HashMap<String, List<HasMapOfMap>>();
    }

    @Test
    public void testMapOfMap() throws Exception {
        HasMapOfMap hmom = new HasMapOfMap();
        final Map<String, String> dmap = new HashMap<String, String>();
        hmom.mom.put("root", dmap);
        dmap.put("deep", "values");
        dmap.put("peer", "lame");

        ds.save(hmom);
        hmom = ds.find(HasMapOfMap.class).get();
        Assert.assertNotNull(hmom.mom);
        Assert.assertNotNull(hmom.mom.get("root"));
        Assert.assertNotNull(hmom.mom.get("root").get("deep"));
        Assert.assertEquals("values", hmom.mom.get("root").get("deep"));
        Assert.assertNotNull("lame", hmom.mom.get("root").get("peer"));
    }

    @Test
    public void testMapOfList() throws Exception {
        HasMapOfList hmol = new HasMapOfList();
        hmol.mol.put("entry1", Collections.singletonList("val1"));
        hmol.mol.put("entry2", Collections.singletonList("val2"));

        ds.save(hmol);
        hmol = ds.find(HasMapOfList.class).get();
        Assert.assertNotNull(hmol.mol);
        Assert.assertNotNull(hmol.mol.get("entry1"));
        Assert.assertNotNull(hmol.mol.get("entry1").get(0));
        Assert.assertEquals("val1", hmol.mol.get("entry1").get(0));
        Assert.assertNotNull("val2", hmol.mol.get("entry2").get(0));
    }

    @Test
    public void testMapOfListOfMapMap() throws Exception {
        final HasMapOfMap hmom = new HasMapOfMap();
        final Map<String, String> dmap = new HashMap<String, String>();
        hmom.mom.put("root", dmap);
        dmap.put("deep", "values");
        dmap.put("peer", "lame");


        HasMapOfListOfMapMap hmolomm = new HasMapOfListOfMapMap();
        hmolomm.mol.put("r1", Collections.singletonList(hmom));
        hmolomm.mol.put("r2", Collections.singletonList(hmom));

        ds.save(hmolomm);
        hmolomm = ds.find(HasMapOfListOfMapMap.class).get();
        Assert.assertNotNull(hmolomm.mol);
        Assert.assertNotNull(hmolomm.mol.get("r1"));
        Assert.assertNotNull(hmolomm.mol.get("r1").get(0));
        Assert.assertNotNull(hmolomm.mol.get("r1").get(0).mom);
        Assert.assertEquals("values", hmolomm.mol.get("r1").get(0).mom.get("root").get("deep"));
        Assert.assertEquals("lame", hmolomm.mol.get("r1").get(0).mom.get("root").get("peer"));
        Assert.assertEquals("values", hmolomm.mol.get("r2").get(0).mom.get("root").get("deep"));
        Assert.assertEquals("lame", hmolomm.mol.get("r2").get(0).mom.get("root").get("peer"));
    }
}
