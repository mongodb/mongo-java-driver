// ClassMapTest.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *  
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.bson.util;

import java.io.Serializable;
import org.testng.annotations.Test;

public class ClassMapTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test
    public void test(){
	// maps Classes to Strings
	ClassMap<String> m = new ClassMap<String>();

	m.put(Object.class, "Object");
	m.put(Boolean.class, "Boolean");
	assertEquals(m.get(Object.class), "Object");
	assertEquals(m.get(Boolean.class), "Boolean");
	assertEquals(m.get(Integer.class), "Object");

	m.put(String.class, "String");
	m.put(Serializable.class, "Serializable");

	assertEquals(m.get(String.class), "String");
	assertEquals(m.get(Integer.class), "Serializable");

	m.put(Number.class, "Number");
	assertEquals(m.get(Integer.class), "Number");

	m.put(Integer.class, "Integer");
	assertEquals(m.get(Integer.class), "Integer");
    }

    public static void main( String args[] ){
	ClassMapTest t = new ClassMapTest();
	t.runConsole();
    }
}
