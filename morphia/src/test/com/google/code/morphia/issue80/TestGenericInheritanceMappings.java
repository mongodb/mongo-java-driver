package com.google.code.morphia.issue80;
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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
 * @author Scott Hernandez
 */
public class TestGenericInheritanceMappings extends TestBase {

	static class GenericHolder<T> {
		@Property
		T thing;
	}
	
	@Embedded
	static class HoldsAString extends GenericHolder<String>{}

	@Embedded
	static class HoldsAnInteger extends GenericHolder<Integer>{}
	
	@Entity
	static class ContainsThings {
		@Id String id;
		HoldsAString stringThing;
		HoldsAnInteger integerThing;
	}
	
	
	@Before @Override
	public void setUp() {
		super.setUp();
		morphia.map(HoldsAnInteger.class).map(HoldsAString.class).map(ContainsThings.class);
	}

	//Waiting on issue 80
    @Test @Ignore
    public void testIt() throws Exception {
    	ContainsThings ct = new ContainsThings();
    	HoldsAnInteger hai = new HoldsAnInteger();
    	hai.thing = 7;
    	HoldsAString has = new HoldsAString();
    	has.thing = "tr";
    	ct.stringThing = has;
    	ct.integerThing = hai;
    	
    	ds.save(ct);
    	assertNotNull(ct.id);
    	assertEquals(ds.getCount(ContainsThings.class), 1);    	
    	ContainsThings ctLoaded = ds.find(ContainsThings.class).get();
    	assertNotNull(ctLoaded);
    	assertNotNull(ctLoaded.id);
    	assertNotNull(ctLoaded.stringThing);
    	assertNotNull(ctLoaded.integerThing);
    }
    
}
