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

package com.google.code.morphia;

import com.google.code.morphia.annotations.AlsoLoad;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class SingleToMultipleConversionTest extends TestBase {
    @Embedded
    private static class HasString {
        String s = "foo";
    }

    @Entity(value = "B", noClassnameStored = true)
    private static class HasEmbeddedStringy {
        @Id
        ObjectId id;
        HasString hs = new HasString();
    }

    @Entity(value = "B", noClassnameStored = true)
    private static class HasEmbeddedStringyArray {
        @Id
        ObjectId id;
        @AlsoLoad("hs")
        HasString[] hss;
    }

    @Entity(value = "B", noClassnameStored = true)
    private static class HasEmbeddedStringySet {
        @Id
        ObjectId id;
        @AlsoLoad("hs")
        Set<HasString> hss;
    }


    @Entity(value = "A", noClassnameStored = true)
    private static class HasSingleString {
        @Id
        ObjectId id;
        String s = "foo";
    }

    @Entity(value = "A", noClassnameStored = true)
    private static class HasManyStringsArray {
        @Id
        ObjectId id;
        @AlsoLoad("s")
        String[] strings;
    }

    @Entity(value = "A", noClassnameStored = true)
    private static class HasManyStringsList {
        @Id
        ObjectId id;
        @AlsoLoad("s")
        List<String> strings;
    }

    @Test
    public void testBasicType() throws Exception {
        ds.delete(ds.createQuery(HasSingleString.class));
        ds.save(new HasSingleString());
        Assert.assertNotNull(ds.find(HasSingleString.class).get());
        Assert.assertEquals(1, ds.find(HasSingleString.class).countAll());
        final HasManyStringsArray hms = ds.find(HasManyStringsArray.class).get();
        Assert.assertNotNull(hms);
        Assert.assertNotNull(hms.strings);
        Assert.assertEquals(1, hms.strings.length);

        final HasManyStringsList hms2 = ds.find(HasManyStringsList.class).get();
        Assert.assertNotNull(hms2);
        Assert.assertNotNull(hms2.strings);
        Assert.assertEquals(1, hms2.strings.size());
    }

    @Test
    public void testEmbeddedType() throws Exception {
        ds.save(new HasEmbeddedStringy());
        Assert.assertNotNull(ds.find(HasEmbeddedStringy.class).get());
        Assert.assertEquals(1, ds.find(HasEmbeddedStringy.class).countAll());
        final HasEmbeddedStringyArray hesa = ds.find(HasEmbeddedStringyArray.class).get();
        Assert.assertNotNull(hesa);
        Assert.assertNotNull(hesa.hss);
        Assert.assertEquals(1, hesa.hss.length);

        final HasEmbeddedStringySet hesa2 = ds.find(HasEmbeddedStringySet.class).get();
        Assert.assertNotNull(hesa2);
        Assert.assertNotNull(hesa2.hss);
        Assert.assertEquals(1, hesa2.hss.size());
    }
}
