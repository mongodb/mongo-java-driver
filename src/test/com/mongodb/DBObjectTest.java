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

package com.mongodb;

import java.net.*;
import java.util.*;

import org.testng.annotations.Test;

import com.mongodb.util.*;

public class DBObjectTest extends TestCase {

    @Test(groups = {"basic"})
    public void testBasicDBObjectCTOR()  {
        Map m = new HashMap();
        m.put("key", "value");
        m.put("foo", 1);
        m.put("bar", null);

        DBObject obj = new BasicDBObject(m);
        assertEquals(obj.get("key"), "value");
        assertEquals(obj.get("foo"), 1);
        assertEquals(obj.get("bar"), null);
    }

    @Test(groups = {"basic"})
    public void testDBObjectBuilder() {
        Map m = new HashMap();
        m.put("foo", "bar");

        BasicDBObjectBuilder b = BasicDBObjectBuilder.start(m);
        b.add("bar", "baz");

        DBObject obj = b.get();
        assertEquals(obj.get("foo"), "bar");
        assertEquals(obj.get("bar"), "baz");

    }

    public static void main( String args[] ) {
        (new DBObjectTest()).runConsole();
    }
}

