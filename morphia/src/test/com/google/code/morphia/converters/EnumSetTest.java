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
package com.google.code.morphia.converters;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.EnumSet;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class EnumSetTest extends TestBase {
    public enum NastyEnum {
        A {
            @Override
            public String toString() {
                return "Never use toString for other purposes than debugging";
            }
        },
        B {
            public String toString() {
                return "Never use toString for other purposes than debugging ";
            }
        },
        C,
        D
    }

    public static class NastyEnumEntity extends TestEntity {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private final EnumSet<NastyEnum> in = EnumSet.of(NastyEnum.B, NastyEnum.C, NastyEnum.D);
        private final EnumSet<NastyEnum> out = EnumSet.of(NastyEnum.A);
        private final EnumSet<NastyEnum> empty = EnumSet.noneOf(NastyEnum.class);
        private EnumSet<NastyEnum> isNull;
    }

    @Test
    public void testNastyEnumPerisistence() throws Exception {
        NastyEnumEntity n = new NastyEnumEntity();
        ds.save(n);
        n = ds.get(n);

        Assert.assertNull(n.isNull);
        Assert.assertNotNull(n.empty);
        Assert.assertNotNull(n.in);
        Assert.assertNotNull(n.out);

        Assert.assertEquals(0, n.empty.size());
        Assert.assertEquals(3, n.in.size());
        Assert.assertEquals(1, n.out.size());

        Assert.assertTrue(n.in.contains(NastyEnum.B));
        Assert.assertTrue(n.in.contains(NastyEnum.C));
        Assert.assertTrue(n.in.contains(NastyEnum.D));
        Assert.assertFalse(n.in.contains(NastyEnum.A));

        Assert.assertTrue(n.out.contains(NastyEnum.A));
        Assert.assertFalse(n.out.contains(NastyEnum.B));
        Assert.assertFalse(n.out.contains(NastyEnum.C));
        Assert.assertFalse(n.out.contains(NastyEnum.D));

        Query<NastyEnumEntity> q = ds.find(NastyEnumEntity.class, "in", NastyEnum.C);
        Assert.assertEquals(1, q.countAll());
        q = ds.find(NastyEnumEntity.class, "out", NastyEnum.C);
        Assert.assertEquals(0, q.countAll());

    }
}
