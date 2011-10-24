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

import java.net.UnknownHostException;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.testng.annotations.Test;

import com.mongodb.util.TestCase;

@SuppressWarnings( { "unchecked" , "deprecation" } )
public class LazyDBObjectTest extends TestCase {

    public LazyDBObjectTest(){
        super();
        try {
            cleanupMongo = new Mongo( "127.0.0.1" );
            cleanupDB = "com_mongodb_unittest_LazyDBObjectTest";
            _db = cleanupMongo.getDB( cleanupDB );
            /*            Mongo mongo = new Mongo( "127.0.0.1" );
        String db = "com_mongodb_unittest_LazyDBObjectTest";
        _db = mongo.getDB( db );*/
        }
        catch ( UnknownHostException e ) {
            throw new MongoException( "couldn't connect" );
        }
    }

    @Test( groups = { "reads" } )
    public void testNormalReadAllTypes()
            throws InterruptedException{
        DBCollection coll = _db.getCollection( "test_reads_normal" );
        //coll.setDBDecoderFactory( LazyDBDecoder.FACTORY );
        ObjectId oid = new ObjectId();
        ObjectId test_oid = new ObjectId();
        ObjectId test_ref_id = new ObjectId();
        DBObject test_doc = new BasicDBObject( "abc", "12345" );
        String[] test_arr = new String[] { "foo" , "bar" , "baz" , "x" , "y" , "z" };
        BSONTimestamp test_tsp = new BSONTimestamp();
        Date test_date = new Date();
        Binary test_bin = new Binary( "scott".getBytes() );
        UUID test_uuid = UUID.randomUUID();
        Pattern test_regex = Pattern.compile( "^test.*regex.*xyz$" );
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        b.append( "_id", oid );
        b.append( "null", null );
        b.append( "max", new MaxKey() );
        b.append( "min", new MinKey() );
        b.append( "booleanTrue", true );
        b.append( "booleanFalse", false );
        b.append( "int1", 1 );
        b.append( "int1500", 1500 );
        b.append( "int3753", 3753 );
        b.append( "tsp", test_tsp );
        b.append( "date", test_date );
        b.append( "long5", 5L );
        b.append( "long3254525", 3254525L );
        b.append( "float324_582", 324.582f );
        b.append( "double245_6289", 245.6289 );
        b.append( "oid", test_oid );
        // Symbol wonky
        b.append( "symbol", new Symbol( "foobar" ) );
        // Code wonky
        b.append( "code", new Code( "var x = 12345;"  ) );
        // TODO - Shell doesn't work with Code W/ Scope, return to this test later
        /*
        b.append( "code_scoped", new CodeWScope( "return x * 500;", test_doc ) );*/
        b.append( "str", "foobarbaz" );
        b.append( "ref", new DBRef( _db, "testRef", test_ref_id ) );
        b.append( "object", test_doc );
        b.append( "array", test_arr );
        b.append( "binary", test_bin );
        b.append( "uuid", test_uuid );
        b.append( "regex", test_regex );
        DBObject _d = b.get();
        coll.insert( _d, WriteConcern.SAFE );
        DBObject doc = coll.findOne( new BasicDBObject( "_id", oid ) );
        assertEquals( doc.get( "str" ), "foobarbaz" );
        assertEquals( doc.get( "_id" ), _d.get( "_id" ) );
        assertNull( doc.get( "null" ) );
        // MaxKey and MinKey don't test against themselves correctly at all
        assertEquals( doc.get( "max" ).toString(), "MaxKey" );
        assertEquals( doc.get( "min" ).toString(), "MinKey" );
        assertEquals( doc.get( "booleanTrue" ), true );
        assertEquals( doc.get( "booleanFalse" ), false );
        assertEquals( doc.get( "int1" ), 1 );
        assertEquals( doc.get( "int1500" ), 1500 );
        assertEquals( doc.get( "int3753" ), 3753 );
        assertEquals( doc.get( "tsp" ), test_tsp );
        assertEquals( doc.get( "date" ), test_date );
        assertEquals( doc.get( "long5" ), 5L );
        assertEquals( doc.get( "long3254525" ), 3254525L );
        // Match against what is expected for MongoDB to store the float as
        assertEquals( doc.get( "float324_582" ), 324.5820007324219 );
        assertEquals( doc.get( "double245_6289" ), 245.6289 );
        assertEquals( doc.get( "oid" ), test_oid );
        assertEquals( doc.get( "str" ), "foobarbaz" );
        assertEquals( doc.get( "ref" ), new DBRef( _db, "testRef", test_ref_id ) );
        assertEquals( ( (DBObject) doc.get( "object" ) ).get( "abc" ), test_doc.get( "abc" ) );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "0" ), "foo" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "1" ), "bar" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "2" ), "baz" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "3" ), "x" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "4" ), "y" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "5" ), "z" );
        assertEquals( new String((byte[]) doc.get( "binary" )), new String(test_bin.getData()));
        assertEquals( doc.get( "uuid" ).toString(), test_uuid.toString() );
        assertEquals( ( (Pattern) doc.get( "regex" ) ).pattern(), test_regex.pattern() );
        assertEquals( ( (Pattern) doc.get( "regex" ) ).flags(), test_regex.flags() );
    }

    @Test( groups = { "reads" } )
    public void testLazyReadAllTypes()
            throws InterruptedException{
        DBCollection coll = _db.getCollection( "test_reads_lazy" );
        coll.setDBDecoderFactory( LazyDBDecoder.FACTORY );
        ObjectId oid = new ObjectId();
        ObjectId test_oid = new ObjectId();
        ObjectId test_ref_id = new ObjectId();
        DBObject test_doc = new BasicDBObject( "abc", "12345" );
        String[] test_arr = new String[] { "foo" , "bar" , "baz" , "x" , "y" , "z" };
        BSONTimestamp test_tsp = new BSONTimestamp();
        Date test_date = new Date();
        Binary test_bin = new Binary( "scott".getBytes() );
        UUID test_uuid = UUID.randomUUID();
        Pattern test_regex = Pattern.compile( "^test.*regex.*xyz$" );
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        b.append( "_id", oid );
        b.append( "null", null );
        b.append( "max", new MaxKey() );
        b.append( "min", new MinKey() );
        b.append( "booleanTrue", true );
        b.append( "booleanFalse", false );
        b.append( "int1", 1 );
        b.append( "int1500", 1500 );
        b.append( "int3753", 3753 );
        b.append( "tsp", test_tsp );
        b.append( "date", test_date );
        b.append( "long5", 5L );
        b.append( "long3254525", 3254525L );
        b.append( "float324_582", 324.582f );
        b.append( "double245_6289", 245.6289 );
        b.append( "oid", test_oid );
        // Symbol wonky
        b.append( "symbol", new Symbol( "foobar" ) );
        // Code wonky
        b.append( "code", new Code( "var x = 12345;"  ) );
        // TODO - Shell doesn't work with Code W/ Scope, return to this test later
        /*
        b.append( "code_scoped", new CodeWScope( "return x * 500;", test_doc ) );*/
        b.append( "str", "foobarbaz" );
        b.append( "ref", new DBRef( _db, "testRef", test_ref_id ) );
        b.append( "object", test_doc );
        b.append( "array", test_arr );
        b.append( "binary", test_bin );
        b.append( "uuid", test_uuid );
        b.append( "regex", test_regex );
        DBObject _d = b.get();
        coll.insert( _d, WriteConcern.SAFE );
        DBObject doc = coll.find( new BasicDBObject( "_id", oid ) ).setDecoderFactory( LazyDBDecoder.FACTORY ).next();
        assertTrue( doc instanceof LazyDBObject, "Did Not Receive a LazyDBObject on read." );
        assertEquals( doc.get( "str" ), "foobarbaz" );
        assertEquals( doc.get( "_id" ), oid );
        assertNull( doc.get( "null" ) );
        // MaxKey and MinKey don't test against themselves correctly at all
        assertEquals( doc.get( "max" ).toString(), "MaxKey" );
        assertEquals( doc.get( "min" ).toString(), "MinKey" );
        assertEquals( doc.get( "booleanTrue" ), true );
        assertEquals( doc.get( "booleanFalse" ), false );
        assertEquals( doc.get( "int1" ), 1 );
        assertEquals( doc.get( "int1500" ), 1500 );
        assertEquals( doc.get( "int3753" ), 3753 );
        assertEquals( doc.get( "tsp" ), test_tsp );
        assertEquals( doc.get( "date" ), test_date );
        assertEquals( doc.get( "long5" ), 5L );
        assertEquals( doc.get( "long3254525" ), 3254525L );
        // Match against what is expected for MongoDB to store the float as
        assertEquals( doc.get( "float324_582" ), 324.5820007324219 );
        assertEquals( doc.get( "double245_6289" ), 245.6289 );
        assertEquals( doc.get( "oid" ), test_oid );
        assertEquals( doc.get( "str" ), "foobarbaz" );
        assertEquals( doc.get( "ref" ), new DBRef( _db, "testRef", test_ref_id ) );
        assertEquals( ( (DBObject) doc.get( "object" ) ).get( "abc" ), test_doc.get( "abc" ) );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "0" ), "foo" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "1" ), "bar" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "2" ), "baz" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "3" ), "x" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "4" ), "y" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "5" ), "z" );
        assertEquals( new String((byte[]) doc.get( "binary" )), new String(test_bin.getData()));
        assertEquals( doc.get( "uuid" ).toString(), test_uuid.toString() );
        assertEquals( ( (Pattern) doc.get( "regex" ) ).pattern(), test_regex.pattern() );
        assertEquals( ( (Pattern) doc.get( "regex" ) ).flags(), test_regex.flags() );
        // Test iteration of keyset
        for (String key : ((LazyDBObject) doc).keySet()) {
            assertNotNull( key );
            if (!key.equals( "null" ))
                assertNotNull( doc.get( key ) );
        }
    }


    private DB _db;

    public static void main( String args[] ){
        ( new LazyDBObjectTest() ).runConsole();
    }
}

