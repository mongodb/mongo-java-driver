/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.bson.BSON;
import org.bson.BsonUndefined;
import org.bson.Transformer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("deprecation")
public class JSONCallbackTest {

    @Test
    public void dateParsing() {

        SimpleDateFormat format = new SimpleDateFormat(JSONCallback._msDateFormat);
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));

        Date rightNow = new Date();
        rightNow.setTime(System.currentTimeMillis());

        Date parsedDate = (Date) JSON.parse("{ \"$date\" : " + rightNow.getTime() + "}");
        assertEquals(0, rightNow.compareTo(parsedDate));

        // Test formatted dates with ms granularity
        parsedDate = (Date) JSON.parse("{ \"$date\" : \"" + format.format(rightNow) + "\"}");
        assertEquals(0, parsedDate.compareTo(format.parse(format.format(rightNow), new ParsePosition(0))));

        // Test formatted dates with sec granularity
        format = new SimpleDateFormat(JSONCallback._secDateFormat);
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));

        parsedDate = (Date) JSON.parse("{ \"$date\" : \"" + format.format(rightNow) + "\"}");
        assertEquals(0, parsedDate.compareTo(format.parse(format.format(rightNow), new ParsePosition(0))));

    }

    @Test
    public void encodingHooks() {
        BSON.addDecodingHook(Date.class, new Transformer() {
            @Override
            public Object transform(final Object o) {
                return ((Date) o).getTime();
            }
        });

        try {
            Date now = new Date();

            Object parsedDate = JSON.parse("{ \"$date\" : " + now.getTime() + "}");
            assertEquals(Long.class, parsedDate.getClass());

            DBObject doc = (DBObject) JSON.parse("{ date : { \"$date\" : " + now.getTime() + "} }");
            assertEquals(Long.class, doc.get("date").getClass());
        } finally {
            BSON.removeDecodingHooks(Date.class);
        }
    }

    @Test
    public void binaryParsing() {
        Binary parsedBinary = (Binary) JSON.parse(("{ \"$binary\" : \"YWJjZA==\", \"$type\" : 0 }"));
        assertEquals(0, parsedBinary.getType());
        assertArrayEquals(new byte[]{97, 98, 99, 100}, parsedBinary.getData());

        Binary parsedBinaryWithHexType = (Binary) JSON.parse(("{ \"$binary\" : \"YWJjZA==\", \"$type\" : \"80\" }"));
        assertEquals((byte) 128, parsedBinaryWithHexType.getType());
        assertArrayEquals(new byte[]{97, 98, 99, 100}, parsedBinaryWithHexType.getData());
    }

    @Test
    public void timestampParsing() {
        BSONTimestamp timestamp = (BSONTimestamp) JSON.parse(("{ \"$timestamp\" : { \"t\": 123, \"i\": 456 } }"));
        assertEquals(456, timestamp.getInc());
        assertEquals(123, timestamp.getTime());
    }


    @Test
    public void regexParsing() {
        Pattern pattern = (Pattern) JSON.parse(("{ \"$regex\" : \".*\",  \"$options\": \"i\" }"));
        assertEquals(".*", pattern.pattern());
        assertEquals(Pattern.CASE_INSENSITIVE, pattern.flags());
    }

    @Test
    public void oidParsing() {
        ObjectId id = (ObjectId) JSON.parse(("{ \"$oid\" : \"01234567890123456789abcd\" }"));
        assertEquals(new ObjectId("01234567890123456789abcd"), id);
    }

    @Test
    public void refParsing() {
        DBRef ref = (DBRef) JSON.parse(("{ \"$ref\" : \"friends\", \"$id\" : { \"$oid\" : \"01234567890123456789abcd\" } }"));
        assertEquals("friends", ref.getCollectionName());
        assertEquals(new ObjectId("01234567890123456789abcd"), ref.getId());
    }

    @Test
    public void undefinedParsing() {
        Object undefined = JSON.parse("{ \"$undefined\" : true }");
        assertEquals(new BsonUndefined(), undefined);
    }

    @Test
    public void numberLongParsing() {
        Long number = (Long) JSON.parse(("{ \"$numberLong\" : \"123456\" }"));
        assertEquals(number, Long.valueOf("123456"));

    }

    @Test
    public void numberDecimalParsing() {
        Decimal128 number = (Decimal128) JSON.parse(("{\"$numberDecimal\" : \"314E-2\"}"));
        assertEquals(number, Decimal128.parse("314E-2"));
    }
}
