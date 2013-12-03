package com.mongodb.util;

import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.bson.BSON;
import org.bson.Transformer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;

public class JSONCallbackTest extends com.mongodb.util.TestCase {

    @Test
    public void dateParsing() {

        SimpleDateFormat format = new SimpleDateFormat(JSONCallback._msDateFormat);
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));

        Date rightNow = new Date();
        rightNow.setTime(System.currentTimeMillis());

        Date parsedDate = (Date) JSON.parse("{ \"$date\" : " + rightNow.getTime() + "}");
        assertEquals(rightNow.compareTo(parsedDate), 0);

        // Test formatted dates with ms granularity
        parsedDate = (Date) JSON.parse("{ \"$date\" : \"" + format.format(rightNow) + "\"}");
        assertEquals(
                parsedDate.compareTo(format.parse(format.format(rightNow), new ParsePosition(0))), 0);

        // Test formatted dates with sec granularity
        format = new SimpleDateFormat(JSONCallback._secDateFormat);
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));

        parsedDate = (Date) JSON.parse("{ \"$date\" : \"" + format.format(rightNow) + "\"}");
        assertEquals(
                parsedDate.compareTo(format.parse(format.format(rightNow), new ParsePosition(0))), 0);

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
        assertEquals(parsedBinary.getType(), 0);
        assertArrayEquals(parsedBinary.getData(), new byte[]{97, 98, 99, 100});
    }

    @Test
    public void timestampParsing() {
        BSONTimestamp timestamp = (BSONTimestamp) JSON.parse(("{ \"$timestamp\" : { \"t\": 123, \"i\": 456 } }"));
        assertEquals(timestamp.getInc(), 456);
        assertEquals(timestamp.getTime(), 123);
    }


    @Test
    public void regexParsing() {
        Pattern pattern = (Pattern) JSON.parse(("{ \"$regex\" : \".*\",  \"$options\": \"i\" }"));
        assertEquals(pattern.pattern(), ".*");
        assertEquals(pattern.flags(), Pattern.CASE_INSENSITIVE);
    }

    @Test
    public void oidParsing() {
        ObjectId id = (ObjectId) JSON.parse(("{ \"$oid\" : \"01234567890123456789abcd\" }"));
        assertEquals(id, new ObjectId("01234567890123456789abcd"));
    }

    @Test
    public void refParsing() {
        DBRef ref = (DBRef) JSON.parse(("{ \"$ref\" : \"friends\", \"$id\" : \"01234567890123456789abcd\" }"));
        assertEquals(ref.getRef(), "friends");
        assertEquals(ref.getId(), new ObjectId("01234567890123456789abcd").toHexString());
    }

// No such concept in Java
//    @Test
//    public void undefinedParsing() {
//        BasicDBObject undef = (BasicDBObject) JSON.parse(("{ \"$undefined\" : true }"));
//        assertEquals(undef, 123);
//    }

}
