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
package com.google.code.morphia.mapping.validation.classrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings("unchecked")
public class RegisterAfterUseTest extends TestBase {

    static class Broken extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Property("foo")
        @Embedded("bar")
        private ArrayList l;
    }

    @Test
    @Ignore(value = "not yet implemented")
    public void testRegisterAfterUse() throws Exception {

        // this would have failed: morphia.map(Broken.class);

        final Broken b = new Broken();
        ds.save(b); // imho must not work
        Assert.fail();

        // doe not revalidate due to being used already!
        morphia.map(Broken.class);
        Assert.fail();
    }
}
