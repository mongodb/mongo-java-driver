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

package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author scotthernandez
 */
public class ReferencesWIgnoreMissingTest extends TestBase {
    @Entity
    static class Container {
        @Id
        private ObjectId id;
        @Reference(ignoreMissing = true)
        private StringHolder[] refs = null;
    }

    @Entity
    static class StringHolder {
        @Id
        private final ObjectId id = new ObjectId();
    }

    @Test
    public void testMissingReference() throws Exception {
        final Container c = new Container();
        c.refs = new StringHolder[]{new StringHolder(), new StringHolder()};
        ds.save(c);
        ds.save(c.refs[0]);

        Container reloadedContainer = ds.find(Container.class).get();
        Assert.assertNotNull(reloadedContainer);
        Assert.assertNotNull(reloadedContainer.refs);
        Assert.assertEquals(1, reloadedContainer.refs.length);

        reloadedContainer = ds.get(c);
        Assert.assertNotNull(reloadedContainer);
        Assert.assertNotNull(reloadedContainer.refs);
        Assert.assertEquals(1, reloadedContainer.refs.length);

        final List<Container> cs = ds.find(Container.class).asList();
        Assert.assertNotNull(cs);
        Assert.assertEquals(1, cs.size());

    }
}
