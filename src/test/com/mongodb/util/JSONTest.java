// JSONTest.java

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

package com.mongodb.util;

import java.util.*;

import java.util.regex.*;

import java.text.*;

import org.bson.types.*;

import com.mongodb.*;

import org.testng.annotations.Test;

public class JSONTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test(groups = {"basic"})
    public void testNumbers(){
        assertEquals(JSON.serialize(JSON.parse("{'x' : 5 }")), "{ \"x\" : 5}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 5.0 }")), "{ \"x\" : 5.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 0 }")), "{ \"x\" : 0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 0.0 }")), "{ \"x\" : 0.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 500 }")), "{ \"x\" : 500}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 500.0 }")), "{ \"x\" : 500.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 0.500 }")), "{ \"x\" : 0.5}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 5. }")), "{ \"x\" : 5.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 5.0e+1 }")), "{ \"x\" : 50.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 5.0E-1 }")), "{ \"x\" : 0.5}");
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void testLongValues() {
        Long val = Integer.MAX_VALUE + 1L; 
        String test = String.format("{ \"x\" : %d}", val);
        assertEquals(JSON.serialize(JSON.parse(test)), test);
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void testSimple() {
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : true}")), "{ \"csdf\" : true}");
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : false}")), "{ \"csdf\" : false}");
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : null}")), "{ \"csdf\" :  null }");
    }
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void testString() {
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : \"foo\"}")), "{ \"csdf\" : \"foo\"}") ;
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : \'foo\'}")), "{ \"csdf\" : \"foo\"}") ;
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : \"a\\\"b\"}")), "{ \"csdf\" : \"a\\\"b\"}");
        assertEquals(JSON.serialize(JSON.parse("{\n\t\"id\":\"1689c12eb234c54a84ebd100\",\n}")),
                     "{ \"id\" : \"1689c12eb234c54a84ebd100\"}");
    }
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void testArray() {
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : [\"foo\"]}")), "{ \"csdf\" : [ \"foo\"]}") ;
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : [3, 5, \'foo\', null]}")), "{ \"csdf\" : [ 3 , 5 , \"foo\" ,  null ]}") ;
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : [3.0, 5.0, \'foo\', null]}")), "{ \"csdf\" : [ 3.0 , 5.0 , \"foo\" ,  null ]}") ;
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : [[],[[]],false]}")), "{ \"csdf\" : [ [ ] , [ [ ]] , false]}");
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void testObject() {
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : {}}")), "{ \"csdf\" : { }}") ;
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : {\"foo\":\"bar\"}}")), "{ \"csdf\" : { \"foo\" : \"bar\"}}") ;
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : {\'hi\':{\'hi\':[{}]}}}")), "{ \"csdf\" : { \"hi\" : { \"hi\" : [ { }]}}}");
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void testMulti() {
        assertEquals(JSON.serialize(JSON.parse("{\'\' : \"\", \"34\" : -52.5}")), "{ \"\" : \"\" , \"34\" : -52.5}") ;
    }    

    @org.testng.annotations.Test(groups = {"basic"})
    public void testUnicode() {
        assertEquals(JSON.serialize(JSON.parse("{'x' : \"hi\\u0020\"}")),"{ \"x\" : \"hi \"}") ;
        assertEquals(JSON.serialize(JSON.parse("{ \"x\" : \"\\u0E01\\u2702\\uF900\"}")), "{ \"x\" : \"\u0E01\u2702\uF900\"}");
        assertEquals(JSON.serialize(JSON.parse("{ \"x\" : \"foo\\u0020bar\"}")), "{ \"x\" : \"foo bar\"}");
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void testBin() {
        byte b[] = {'a', 'b', 0, 'd'};
        DBObject obj = BasicDBObjectBuilder.start().add("b", b).get();
        assertEquals(JSON.serialize(obj), "{ \"b\" : <Binary Data>}");
    }


    @org.testng.annotations.Test(groups = {"basic"})
    public void testErrors(){
        boolean threw = false;
        try {
            JSON.parse("{\"x\" : \"");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, true);
        threw = false;
        try {
            JSON.parse("{\"x\" : \"\\");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, true);
        threw = false;
        try {
            JSON.parse("{\"x\" : 5.2");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, true);
        threw = false;
        try {
            JSON.parse("{\"x\" : 5");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, true);
        threw = false;
        try {
            JSON.parse("{\"x\" : 5,");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, true);
        threw = false;
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void testBasic(){
        assertEquals( JSON.serialize(JSON.parse("{}")), "{ }");
        assertEquals( JSON.parse(""), null );
        assertEquals( JSON.parse("     "), null);
        assertEquals( JSON.parse(null), null);

        boolean threw = false;
        try {
            JSON.parse("{");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, true);
        threw = false;

        try {
            JSON.parse("}");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, true);
        threw = false;

        try {
            JSON.parse("{{}");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, true);
        threw = false;

        try {
            JSON.parse("4");
        }
        catch(JSONParseException e) {
            threw = true;
        }
        assertEquals(threw, false);
        threw = false;
        
        assertEquals( 4 , JSON.parse( "4" ) );
    }

    @org.testng.annotations.Test    
    public void testNumbers2(){
        DBObject x = new BasicDBObject( "x" , 123 );
        assertEquals( x , JSON.parse( x.toString() ) );

        x = new BasicDBObject( "x" , 123123123123L );
        assertEquals( x , JSON.parse( x.toString() ) );

        x = new BasicDBObject( "x" , 123123123 );
        assertEquals( x , JSON.parse( x.toString() ) );
    }

    void _escapeChar( String s ){
        String thingy = "va" + s + "lue";
        DBObject x = new BasicDBObject( "name" , thingy );
        x = (DBObject)JSON.parse( x.toString() );
        assertEquals( thingy , x.get( "name" ) );

        thingy = "va" + s + s + s + "lue" + s;
        x = new BasicDBObject( "name" , thingy );
        x = (DBObject)JSON.parse( x.toString() );
        assertEquals( thingy , x.get( "name" ) );
    }



    @org.testng.annotations.Test    
    public void testEscape1(){
        String raw = "a\nb";

        DBObject x = new BasicDBObject( "x" , raw );
        assertEquals( "\"a\\nb\"" , JSON.serialize( raw ) );
        assertEquals( x , JSON.parse( x.toString() ) );
        assertEquals( raw , ((DBObject)JSON.parse( x.toString() ) ).get( "x" ) );

        x = new BasicDBObject( "x" , "a\nb\bc\td\re" );
        assertEquals( x , JSON.parse( x.toString() ) );

        
        String thingy = "va\"lue";
        x = new BasicDBObject( "name" , thingy );
        x = (DBObject)JSON.parse( x.toString() );
        assertEquals( thingy , x.get( "name" ) );

        thingy = "va\\lue";
        x = new BasicDBObject( "name" , thingy );
        x = (DBObject)JSON.parse( x.toString() );
        assertEquals( thingy , x.get( "name" ) );

        assertEquals( "va/lue" , (String)JSON.parse("\"va\\/lue\"") );
        assertEquals( "value" , (String)JSON.parse("\"va\\lue\"") );
        assertEquals( "va\\lue" , (String)JSON.parse("\"va\\\\lue\"") );

        _escapeChar( "\t" );
        _escapeChar( "\b" );
        _escapeChar( "\n" );
        _escapeChar( "\r" );
        _escapeChar( "\'" );
        _escapeChar( "\"" );
        _escapeChar( "\\" );
    }

   @org.testng.annotations.Test
   public void testPattern() {
       String x = "^Hello$";
       String serializedPattern = 
	   "{ \"$regex\" : \"" + x + "\" , \"$options\" : \"" + "i\"}";
       
       Pattern pattern = Pattern.compile( x , Pattern.CASE_INSENSITIVE);
       assertEquals( serializedPattern, JSON.serialize(pattern));

       BasicDBObject a = new BasicDBObject( "x" , pattern );
       assertEquals( "{ \"x\" : " + serializedPattern + "}" , a.toString() );

       DBObject b = (DBObject)JSON.parse( a.toString() );
       assertEquals( b.get("x").getClass(), Pattern.class );
       assertEquals( a.toString() , b.toString() );
   }

   @org.testng.annotations.Test
   public void testObjectId() {
       ObjectId oid = new ObjectId(new Date());

       String serialized = JSON.serialize(oid);
       assertEquals("{ \"$oid\" : \"" + oid + "\"}", serialized);
       
       ObjectId oid2 = (ObjectId)JSON.parse(serialized);
       assertEquals(oid, oid2);
   }

   @org.testng.annotations.Test
   public void testDate() {
       Date d = new Date();
       SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
       format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
       String formattedDate = format.format(d);

       String serialized = JSON.serialize(d);
       assertEquals("{ \"$date\" : \"" + formattedDate + "\"}", serialized);

       Date d2 = (Date)JSON.parse(serialized);
       assertEquals(d.toString(), d2.toString());
   }

    public static void main( String args[] ){
        (new JSONTest()).runConsole();
    }
    
}
