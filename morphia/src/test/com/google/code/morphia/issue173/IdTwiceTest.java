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

package com.google.code.morphia.issue173;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Converters;
import com.google.code.morphia.testutil.TestEntity;
import com.mongodb.WriteConcern;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class IdTwiceTest extends TestBase {

    @Test
    public final void testCalendar() {
        morphia.map(A.class);
        final A a = new A();
        a.c = GregorianCalendar.getInstance();
        ds.save(a, WriteConcern.SAFE);
        // occasionally failed, so i suspected a race cond.
        final A loaded = ds.find(A.class).get();
        Assert.assertNotNull(loaded.c);
        Assert.assertEquals(a.c, loaded.c);
    }

    @Converters(CalendarConverter.class)
    private static class A extends TestEntity {
        private static final long serialVersionUID = 1L;
        Calendar c;
    }

}
