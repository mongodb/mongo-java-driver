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

public class DBRefTest extends TestCase {

    @Test(groups = {"basic"})
    public void testDBRefBaseToString()  
        throws UnknownHostException {
        Mongo db = new Mongo( "127.0.0.1" , "test" );

        ObjectId id = new ObjectId("123456789012345678901234");
        DBRefBase ref = new DBRefBase(db, "foo.bar", id);

        assertEquals("{ \"$ref\" : \"foo.bar\", \"$id\" : \"123456789012345678901234\" }", ref.toString());
    }
    
    public static void main( String args[] ) {
        (new DBRefTest()).runConsole();
    }
}

