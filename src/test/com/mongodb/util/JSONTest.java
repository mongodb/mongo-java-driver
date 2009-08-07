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

import com.mongodb.DBObject;
import com.mongodb.BasicDBObjectBuilder;
import org.testng.annotations.Test;

public class JSONTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test(groups = {"basic"})
    public void testNumbers(){
        assertEquals(JSON.serialize(JSON.parse("{'x' : 5. }")), "{ \"x\" : 5.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 5.0 }")), "{ \"x\" : 5.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 0 }")), "{ \"x\" : 0.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 500 }")), "{ \"x\" : 500.0}");
        assertEquals(JSON.serialize(JSON.parse("{'x' : 0.500 }")), "{ \"x\" : 0.5}");
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
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : \"\\\"\"}")), "{ \"csdf\" : \"\\\\\\\"\"}");
    }
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void testArray() {
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : [\"foo\"]}")), "{ \"csdf\" : [ \"foo\"]}") ;
        assertEquals(JSON.serialize(JSON.parse("{'csdf' : [3, 5, \'foo\', null]}")), "{ \"csdf\" : [ 3.0 , 5.0 , \"foo\" ,  null ]}") ;
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
        assertEquals(threw, true);
        threw = false;
    }



    public static void main( String args[] ){
        (new JSONTest()).runConsole();
    }
    
}
