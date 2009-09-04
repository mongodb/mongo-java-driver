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

import java.util.*;
import java.util.regex.*;
import java.io.IOException;

import org.testng.annotations.Test;

import com.mongodb.util.*;
import com.mongodb.util.gridfs.*;

public class GridFSTest extends TestCase {

    public GridFSTest()
        throws IOException , MongoException {
        super();
        _db = new Mongo( "127.0.0.1" , "cursortest" );
    }

    @Test(groups = {"basic"})
    public void testMetadata() {
        GridFS fs = new GridFS(_db);

        DBObject info = BasicDBObjectBuilder
            .start()
            .add("filename", "src/main/com/mongodb/Mongo.java")
            .get();
        
        DBObject metadata = BasicDBObjectBuilder
            .start()
            .add("foo","bar")
            .add("something","else")
            .get();


        GridFSObject obj = new GridFSObject(fs, info , metadata );
        try {
            fs.write(obj);
        }
        catch (IOException e) {
            assertEquals(0,1);
        }
        
        GridFSObject ret = fs.read("src/main/com/mongodb/Mongo.java");
        DBObject check = ret.getMetadata();
        assertEquals( check.get("foo"), "bar");
        assertEquals( check.get("something"), "else");
    }

    final Mongo _db;

    public static void main( String args[] )
        throws Exception {
        (new GridFSTest()).runConsole();
    }

}
