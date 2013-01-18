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
package com.google.code.morphia.optimisticlocks;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Version;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.ConcurrentModificationException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class VersionTest extends TestBase {


    public static class ALongPrimitive extends TestEntity {
        private static final long serialVersionUID = 1L;

        @Version
        private long hubba;

        private String text;
    }

    public static class ALong extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Version("versionNameContributedByAnnotation")
        private Long v;

        private String text;
    }

    @Entity
    static class InvalidVersionUse {
        @Id
        private String id;
        @Version
        private long version1;
        @Version
        private long version2;

    }

    @Test
    public void testInvalidVersionUse() throws Exception {
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() throws Throwable {
                morphia.map(InvalidVersionUse.class);
            }
        };

    }

    @Test
    public void testVersions() throws Exception {
        final ALongPrimitive a = new ALongPrimitive();
        Assert.assertEquals(0, a.hubba);
        ds.save(a);
        Assert.assertTrue(a.hubba > 0);
        final long version1 = a.hubba;

        ds.save(a);
        Assert.assertTrue(a.hubba > 0);
        final long version2 = a.hubba;

        Assert.assertFalse(version1 == version2);
    }

    @Test
    public void testConcurrentModDetection() throws Exception {
        morphia.map(ALongPrimitive.class);

        final ALongPrimitive a = new ALongPrimitive();
        Assert.assertEquals(0, a.hubba);
        ds.save(a);
        final ALongPrimitive a1 = a;

        final ALongPrimitive a2 = ds.get(a);
        ds.save(a2);


        new AssertedFailure(ConcurrentModificationException.class) {
            public void thisMustFail() throws Throwable {
                ds.save(a1);
            }
        };
    }

    @Test
    public void testConcurrentModDetectionLong() throws Exception {
        final ALong a = new ALong();
        Assert.assertEquals(null, a.v);
        ds.save(a);
        final ALong a1 = a;

        final ALong a2 = ds.get(a);
        ds.save(a2);

        new AssertedFailure(ConcurrentModificationException.class) {
            public void thisMustFail() throws Throwable {
                ds.save(a1);
            }
        };
    }

    @Test
    public void testConcurrentModDetectionLongWithMerge() throws Exception {
        final ALong a = new ALong();
        Assert.assertEquals(null, a.v);
        ds.save(a);
        final ALong a1 = a;

        a1.text = " foosdfds ";
        final ALong a2 = ds.get(a);
        ds.save(a2);

        new AssertedFailure(ConcurrentModificationException.class) {
            public void thisMustFail() throws Throwable {
                ds.merge(a1);
            }
        };
    }

    @Test
    public void testVersionFieldNameContribution() throws Exception {
        final MappedField mappedFieldByJavaField = morphia.getMapper().getMappedClass(ALong.class)
                .getMappedFieldByJavaField("v");
        Assert.assertEquals("versionNameContributedByAnnotation", mappedFieldByJavaField.getNameToStore());
    }

}
