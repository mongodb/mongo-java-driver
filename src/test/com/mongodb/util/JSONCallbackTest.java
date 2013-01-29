package com.mongodb.util;

import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.bson.BSON;
import org.bson.Transformer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.regex.Pattern;

public class JSONCallbackTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test(groups = {"basic"})
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

    @org.testng.annotations.Test(groups = {"basic"})
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

    @org.testng.annotations.Test(groups = {"basic"})
    public void binaryParsing() {
        Binary parsedBinary = (Binary) JSON.parse(("{ \"$binary\" : \"YWJjZA==\", \"$type\" : 0 }"));
        org.testng.Assert.assertEquals(parsedBinary.getType(), 0);
        org.testng.Assert.assertEquals(parsedBinary.getData(), new byte[]{97, 98, 99, 100});
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void timestampParsing() {
        BSONTimestamp timestamp = (BSONTimestamp) JSON.parse(("{ \"$timestamp\" : { \"t\": 123, \"i\": 456 } }"));
        org.testng.Assert.assertEquals(timestamp.getInc(), 456);
        org.testng.Assert.assertEquals(timestamp.getTime(), 123);
    }


    @org.testng.annotations.Test(groups = {"basic"})
    public void regexParsing() {
        Pattern pattern = (Pattern) JSON.parse(("{ \"$regex\" : \".*\",  \"$options\": \"i\" }"));
        org.testng.Assert.assertEquals(pattern.pattern(), ".*");
        org.testng.Assert.assertEquals(pattern.flags(), Pattern.CASE_INSENSITIVE);
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void oidParsing() {
        ObjectId id = (ObjectId) JSON.parse(("{ \"$oid\" : \"01234567890123456789abcd\" }"));
        org.testng.Assert.assertEquals(id, new ObjectId("01234567890123456789abcd"));
    }

    @org.testng.annotations.Test(groups = {"basic"})
    public void refParsing() {
        DBRef ref = (DBRef) JSON.parse(("{ \"$ref\" : \"friends\", \"$id\" : \"01234567890123456789abcd\" }"));
        org.testng.Assert.assertEquals(ref.getRef(), "friends");
        org.testng.Assert.assertEquals(ref.getId(), new ObjectId("01234567890123456789abcd"));
    }

// No such concept in Java
//    @org.testng.annotations.Test(groups = {"basic"})
//    public void undefinedParsing() {
//        BasicDBObject undef = (BasicDBObject) JSON.parse(("{ \"$undefined\" : true }"));
//        org.testng.Assert.assertEquals(undef, 123);
//    }

}
