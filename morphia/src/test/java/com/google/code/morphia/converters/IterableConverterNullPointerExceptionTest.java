package com.google.code.morphia.converters;
/**
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

/**
 *
 */
public class IterableConverterNullPointerExceptionTest extends TestBase
{
	@Entity
	static class TestEntity {
		@Id String id;
		String[] array;
	}

	@Before @Override
	public void setUp() {
		super.setUp();
		morphia.map(TestEntity.class);
	}

	@Test
    public void testIt() throws Exception
    {
        TestEntity te = new TestEntity();
        te.array = new String[]{ null, "notNull", null };
    	ds.save(te);

        TestEntity te2 = null;
        try {
            te2 = ds.find(TestEntity.class).get();
        } catch (RuntimeException e) {
            e.printStackTrace();
            fail();
        }
        assertArrayEquals(te.array, te2.array);
    }
}
