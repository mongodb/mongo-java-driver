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

    public DBRefTest() {
        try {
            _db = new Mongo( "127.0.0.1" , "test" );
        }
        catch(UnknownHostException e) {
            throw new MongoException("couldn't connect");
        }
    }

    @Test(groups = {"basic"})
    public void testDBRefBaseToString(){

        ObjectId id = new ObjectId("123456789012345678901234");
        DBRefBase ref = new DBRefBase(_db, "foo.bar", id);

        assertEquals("{ \"$ref\" : \"foo.bar\", \"$id\" : \"123456789012345678901234\" }", ref.toString());
    }

    @Test(groups = {"basic"})
    public void testDBRef(){

        ByteEncoder encoder = ByteEncoder.get();
        
        DBRef ref = new DBRef(_db, "hello", (Object)"world");
        DBObject o = new BasicDBObject("!", ref);
        
        encoder.putObject( o );
        
        encoder.flip();
        
        ByteDecoder decoder = new ByteDecoder( encoder._buf );
        DBObject read = decoder.readObject();
        
        assertEquals("{ \"!\" : { \"$ref\" : \"hello\" , \"$id\" : \"world\"}}", read.toString());
    }

    @Test(groups = {"basic"})
    public void testDBRefFetches(){

        ByteEncoder encoder = ByteEncoder.get();
        
        BasicDBObject obj = new BasicDBObject("_id", 321325243);
        _db.getCollection("x").save(obj);

        DBRef ref = new DBRef(_db, "x", 321325243);
        DBObject deref = ref.fetch();

        assertTrue(deref != null);
        assertEquals(321325243, ((Number)deref.get("_id")).intValue());

        DBObject refobj = BasicDBObjectBuilder.start().add("$ref", "x").add("$id", 321325243).get();
        deref = DBRef.fetch(_db, refobj);

        assertTrue(deref != null);
        assertEquals(321325243, ((Number)deref.get("_id")).intValue());
    }

    DB _db;

    public static void main( String args[] ) {
        (new DBRefTest()).runConsole();
    }
}

