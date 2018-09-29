/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.util;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class JSONTest {

    @Test
    public void testSerializationMethods() {

        // basic test of each of JSON class' serialization methods
        String json = "{ \"x\" : \"basic test\"}";
        Object obj = JSON.parse(json);

        assertEquals(json, JSON.serialize(obj));
    }

    @Test
    public void testNumbers() {
        assertEquals("{ \"x\" : 5}", JSON.serialize(JSON.parse("{'x' : 5 }")));
        assertEquals("{ \"x\" : 5.0}", JSON.serialize(JSON.parse("{'x' : 5.0 }")));
        assertEquals("{ \"x\" : 0}", JSON.serialize(JSON.parse("{'x' : 0 }")));
        assertEquals("{ \"x\" : 0.0}", JSON.serialize(JSON.parse("{'x' : 0.0 }")));
        assertEquals("{ \"x\" : 500}", JSON.serialize(JSON.parse("{'x' : 500 }")));
        assertEquals("{ \"x\" : 500.0}", JSON.serialize(JSON.parse("{'x' : 500.0 }")));
        assertEquals("{ \"x\" : 0.5}", JSON.serialize(JSON.parse("{'x' : 0.500 }")));
        assertEquals("{ \"x\" : 5.0}", JSON.serialize(JSON.parse("{'x' : 5. }")));
        assertEquals("{ \"x\" : 50.0}", JSON.serialize(JSON.parse("{'x' : 5.0e+1 }")));
        assertEquals("{ \"x\" : 0.5}", JSON.serialize(JSON.parse("{'x' : 5.0E-1 }")));
    }

    @Test
    public void testLongValues() {
        Long bigVal = Integer.MAX_VALUE + 1L;
        String test = String.format("{ \"x\" : %d}", bigVal);
        assertEquals(test, JSON.serialize(JSON.parse(test)));

        Long smallVal = Integer.MIN_VALUE - 1L;
        String test2 = String.format("{ \"x\" : %d}", smallVal);
        assertEquals(test2, JSON.serialize(JSON.parse(test2)));

        try {
            JSON.parse("{\"ReallyBigNumber\": 10000000000000000000 }");
            fail("JSONParseException should have been thrown");
        } catch (JSONParseException e) {
            // fall through
        }
    }

    @Test
    public void testSimple() {
        assertEquals("{ \"csdf\" : true}", JSON.serialize(JSON.parse("{'csdf' : true}")));
        assertEquals("{ \"csdf\" : false}", JSON.serialize(JSON.parse("{'csdf' : false}")));
        assertEquals("{ \"csdf\" :  null }", JSON.serialize(JSON.parse("{'csdf' : null}")));
        assertEquals("{ \"id\" : \"1689c12eb234c54a84ebd100\"}",
                     JSON.serialize(JSON.parse("{\n\t\"id\":\"1689c12eb234c54a84ebd100\",\n}")));
    }

    @Test
    public void testString() {
        assertEquals("{ \"csdf\" : \"foo\"}", JSON.serialize(JSON.parse("{'csdf' : \"foo\"}")));
        assertEquals("{ \"csdf\" : \"foo\"}", JSON.serialize(JSON.parse("{'csdf' : \'foo\'}")));
        assertEquals("{ \"csdf\" : \"a\\\"b\"}", JSON.serialize(JSON.parse("{'csdf' : \"a\\\"b\"}")));
    }

    @Test
    public void testArray() {
        assertEquals("{ \"csdf\" : [ \"foo\"]}", JSON.serialize(JSON.parse("{'csdf' : [\"foo\"]}")));
        assertEquals("{ \"csdf\" : [ 3 , 5 , \"foo\" ,  null ]}", JSON.serialize(JSON.parse("{'csdf' : [3, 5, \'foo\', null]}")));
        assertEquals("{ \"csdf\" : [ 3.0 , 5.0 , \"foo\" ,  null ]}", JSON.serialize(JSON.parse("{'csdf' : [3.0, 5.0, \'foo\', null]}")));
        assertEquals("{ \"csdf\" : [ [ ] , [ [ ]] , false]}", JSON.serialize(JSON.parse("{'csdf' : [[],[[]],false]}")));
    }

    @Test
    public void testObject() {
        assertEquals("{ \"csdf\" : { }}", JSON.serialize(JSON.parse("{'csdf' : {}}")));
        assertEquals("{ \"csdf\" : { \"foo\" : \"bar\"}}", JSON.serialize(JSON.parse("{'csdf' : {\"foo\":\"bar\"}}")));
        assertEquals("{ \"csdf\" : { \"hi\" : { \"hi\" : [ { }]}}}", JSON.serialize(JSON.parse("{'csdf' : {\'hi\':{\'hi\':[{}]}}}")));
    }

    @Test
    public void testMulti() {
        assertEquals("{ \"\" : \"\" , \"34\" : -52.5}", JSON.serialize(JSON.parse("{\'\' : \"\", \"34\" : -52.5}")));
    }

    @Test
    public void testUnicode() {
        assertEquals("{ \"x\" : \"hi \"}", JSON.serialize(JSON.parse("{'x' : \"hi\\u0020\"}")));
        assertEquals("{ \"x\" : \"\u0E01\u2702\uF900\"}", JSON.serialize(JSON.parse("{ \"x\" : \"\\u0E01\\u2702\\uF900\"}")));
        assertEquals("{ \"x\" : \"foo bar\"}", JSON.serialize(JSON.parse("{ \"x\" : \"foo\\u0020bar\"}")));
    }

    @Test
    public void testBin() {
        byte[] b = {'a', 'b', 0, 'd'};
        DBObject obj = BasicDBObjectBuilder.start().add("b", b).get();
        assertEquals("{ \"b\" : <Binary Data>}", JSON.serialize(obj));
    }


    @Test
    public void testErrors() {
        boolean threw = false;
        try {
            JSON.parse("{\"x\" : \"");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(true, threw);
        threw = false;
        try {
            JSON.parse("{\"x\" : \"\\");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(true, threw);
        threw = false;
        try {
            JSON.parse("{\"x\" : 5.2");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(true, threw);
        threw = false;
        try {
            JSON.parse("{\"x\" : 5");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(true, threw);
        threw = false;
        try {
            JSON.parse("{\"x\" : 5,");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(true, threw);
        threw = false;
    }

    @Test
    public void testBasic() {
        assertEquals("{ }", JSON.serialize(JSON.parse("{}")));
        assertEquals(null, JSON.parse(""));
        assertEquals(null, JSON.parse("     "));
        assertEquals(null, JSON.parse(null));

        boolean threw = false;
        try {
            JSON.parse("{");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(true, threw);
        threw = false;

        try {
            JSON.parse("}");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(true, threw);
        threw = false;

        try {
            JSON.parse("{{}");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(true, threw);
        threw = false;

        try {
            JSON.parse("4");
        } catch (JSONParseException e) {
            threw = true;
        }
        assertEquals(false, threw);
        threw = false;

        assertEquals(JSON.parse("4"), 4);
    }

    @Test
    public void testNumbers2() {
        DBObject x = new BasicDBObject("x", 123);
        assertEquals(JSON.parse(x.toString()), x);

        x = new BasicDBObject("x", 123123123123L);
        assertEquals(JSON.parse(x.toString()), x);

        x = new BasicDBObject("x", 123123123);
        assertEquals(JSON.parse(x.toString()), x);
    }

    private void escapeChar(final String s) {
        String thingy = "va" + s + "lue";
        DBObject x = new BasicDBObject("name", thingy);
        x = (DBObject) JSON.parse(x.toString());
        assertEquals(x.get("name"), thingy);

        thingy = "va" + s + s + s + "lue" + s;
        x = new BasicDBObject("name", thingy);
        x = (DBObject) JSON.parse(x.toString());
        assertEquals(x.get("name"), thingy);
    }


    @Test
    public void testEscape1() {
        String raw = "a\nb";

        DBObject x = new BasicDBObject("x", raw);
        assertEquals("\"a\\nb\"", JSON.serialize(raw));
        assertEquals(x, JSON.parse(x.toString()));
        assertEquals(raw, ((DBObject) JSON.parse(x.toString())).get("x"));

        x = new BasicDBObject("x", "a\nb\bc\td\re");
        assertEquals(x, JSON.parse(x.toString()));


        String thingy = "va\"lue";
        x = new BasicDBObject("name", thingy);
        x = (DBObject) JSON.parse(x.toString());
        assertEquals(thingy, x.get("name"));

        thingy = "va\\lue";
        x = new BasicDBObject("name", thingy);
        x = (DBObject) JSON.parse(x.toString());
        assertEquals(thingy, x.get("name"));

        assertEquals("va/lue", JSON.parse("\"va\\/lue\""));
        assertEquals("value", JSON.parse("\"va\\lue\""));
        assertEquals("va\\lue", JSON.parse("\"va\\\\lue\""));

        escapeChar("\t");
        escapeChar("\b");
        escapeChar("\n");
        escapeChar("\r");
        escapeChar("\'");
        escapeChar("\"");
        escapeChar("\\");
    }

    // This is not correct behavior, but adding the test here to document it.  It should probably throw a JSONParseException due to the
    // illegal escape of \m.
    @Test
    public void testIllegalEscape() {
        DBObject obj = (DBObject) JSON.parse("{ 'x' : '\\m' }");
        assertEquals("m", obj.get("x"));
    }

    @Test
    public void testPattern() {
        String x = "^Hello$";
        String serializedPattern =
            "{ \"$regex\" : \"" + x + "\" , \"$options\" : \"" + "i\"}";

        Pattern pattern = Pattern.compile(x, Pattern.CASE_INSENSITIVE);
        assertEquals(serializedPattern, JSON.serialize(pattern));

        BasicDBObject a = new BasicDBObject("x", pattern);
        assertEquals("{ \"x\" : " + serializedPattern + "}", JSON.serialize(a));

        DBObject b = (DBObject) JSON.parse(a.toString());
        assertEquals(Pattern.class, b.get("x").getClass());
        assertEquals(b.toString(), a.toString());
    }

    @Test
    public void testRegexNoOptions() {
        String x = "^Hello$";
        String serializedPattern =
            "{ \"$regex\" : \"" + x + "\"}";

        Pattern pattern = Pattern.compile(x);
        assertEquals(JSON.serialize(pattern), serializedPattern);

        BasicDBObject a = new BasicDBObject("x", pattern);
        assertEquals("{ \"x\" : " + serializedPattern + "}", JSON.serialize(a));

        DBObject b = (DBObject) JSON.parse(a.toString());
        assertEquals(Pattern.class, b.get("x").getClass());
        assertEquals(b.toString(), a.toString());
    }

    @Test
    public void testObjectId() {
        ObjectId oid = new ObjectId(new Date());

        String serialized = JSON.serialize(oid);
        assertEquals("{ \"$oid\" : \"" + oid + "\"}", serialized);

        ObjectId oid2 = (ObjectId) JSON.parse(serialized);
        assertEquals(oid, oid2);
    }

    @Test
    public void testDate() {
        Date d = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        String formattedDate = format.format(d);

        String serialized = JSON.serialize(d);
        assertEquals("{ \"$date\" : \"" + formattedDate + "\"}", serialized);

        Date d2 = (Date) JSON.parse(serialized);
        assertEquals(d2.toString(), d.toString());
        assertTrue(d.equals(d2));
    }

    @Test
    public void testJSONEncoding() throws ParseException {
        String json = "{ 'str' : 'asdfasd' , 'long' : 123123123123 , 'int' : 5 , 'float' : 0.4 , 'bool' : false , "
                      + "'date' : { '$date' : '2011-05-18T18:56:00Z'} , 'pat' : { '$regex' : '.*' , '$options' : ''} , "
                      + "'oid' : { '$oid' : '4d83ab3ea39562db9c1ae2ae'} , 'ref' : { '$ref' : 'test.test' , "
                      + "'$id' : { '$oid' : '4d83ab59a39562db9c1ae2af'}} , 'code' : { '$code' : 'asdfdsa'} , "
                      + "'codews' : { '$code' : 'ggggg' , "
                      + "'$scope' : { }} , 'ts' : { '$ts' : 1300474885 , '$inc' : 10} , 'null' :  null, "
                      + "'uuid' : { '$uuid' : '60f65152-6d4a-4f11-9c9b-590b575da7b5' }}";
        BasicDBObject a = (BasicDBObject) JSON.parse(json);
        assertEquals("asdfasd", a.get("str"));
        assertEquals(5, a.get("int"));
        assertEquals(123123123123L, a.get("long"));
        assertEquals(0.4d, a.get("float"));
        assertEquals(false, a.get("bool"));
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        assertEquals(format.parse("2011-05-18T18:56:00Z"), a.get("date"));
        Pattern pat = (Pattern) a.get("pat");
        Pattern pat2 = Pattern.compile(".*", org.bson.BSON.regexFlags(""));
        assertEquals(pat.pattern(), pat2.pattern());
        assertEquals(pat.flags(), pat2.flags());
        ObjectId oid = (ObjectId) a.get("oid");
        assertEquals(new ObjectId("4d83ab3ea39562db9c1ae2ae"), oid);
        //        DBRef ref = (DBRef) a.get("ref");
        //        assertEquals(new DBRef(null, "test.test", new ObjectId("4d83ab59a39562db9c1ae2af")), ref);
        assertEquals(new Code("asdfdsa"), a.get("code"));
        assertEquals(new CodeWScope("ggggg", new BasicBSONObject()), a.get("codews"));
        assertEquals(new BSONTimestamp(1300474885, 10), a.get("ts"));
        assertEquals(UUID.fromString("60f65152-6d4a-4f11-9c9b-590b575da7b5"), a.get("uuid"));
        String json2 = JSON.serialize(a);
        BasicDBObject b = (BasicDBObject) JSON.parse(json2);
        assertEquals(JSON.serialize(b), JSON.serialize(b));
    }

}
