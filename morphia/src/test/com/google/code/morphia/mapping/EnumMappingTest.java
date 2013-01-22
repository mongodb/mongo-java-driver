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
import com.google.code.morphia.annotations.PreSave;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class EnumMappingTest extends TestBase {
    static class ContainsEnum {
        @Id
        private ObjectId id;
        private final Foo foo = Foo.BAR;

        @PreSave
        void testMapping() {

        }
    }

    static enum Foo {
        BAR() {
        },
        BAZ
    }

    @Test
    public void testEnumMapping() throws Exception {
        morphia.map(ContainsEnum.class);

        ds.save(new ContainsEnum());
        Assert.assertEquals(1, ds.createQuery(ContainsEnum.class).field("foo").equal(Foo.BAR).countAll());
        Assert.assertEquals(1, ds.createQuery(ContainsEnum.class).filter("foo", Foo.BAR).countAll());
        Assert.assertEquals(1, ds.createQuery(ContainsEnum.class).disableValidation().filter("foo",
                                                                                            Foo.BAR).countAll());
    }

}
