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

package com.google.code.morphia.issue325;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


@SuppressWarnings("unused")
public class EmbeddedClassnameTest extends TestBase {

    @Entity(noClassnameStored = true)
    private static class Root {
        @Id
        private final String id = "a";

        @Embedded
        private final List<A> as = new ArrayList<A>();

        @Embedded
        private final List<B> bs = new ArrayList<B>();
    }

    private static class A {
        private final String name = "undefined";

        @Transient
        DBObject raw;

        @PreLoad
        void preLoad(final DBObject dbObj) {
            raw = dbObj;
        }
    }

    private static class B extends A {
        private final String description = "<descr. here>";
    }

    @Test
    public final void testEmbeddedClassname() {
        Root r = new Root();
        ds.save(r);

        final A a = new A();
        ds.update(ds.createQuery(Root.class), ds.createUpdateOperations(Root.class).add("as", a));
        r = ds.get(Root.class, "a");
        Assert.assertFalse(r.as.get(0).raw.containsField(Mapper.CLASS_NAME_FIELDNAME));

        B b = new B();
        ds.update(ds.createQuery(Root.class), ds.createUpdateOperations(Root.class).add("bs", b));
        r = ds.get(Root.class, "a");
        Assert.assertFalse(r.bs.get(0).raw.containsField(Mapper.CLASS_NAME_FIELDNAME));

        ds.delete(ds.createQuery(Root.class));
        //test saving an B in as, and it should have the classname.

        ds.save(new Root());
        b = new B();
        ds.update(ds.createQuery(Root.class), ds.createUpdateOperations(Root.class).add("as", b));
        r = ds.get(Root.class, "a");
        Assert.assertTrue(r.as.get(0).raw.containsField(Mapper.CLASS_NAME_FIELDNAME));

    }

}
