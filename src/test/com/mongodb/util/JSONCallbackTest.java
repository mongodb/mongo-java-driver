package com.mongodb.util;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

public class JSONCallbackTest extends com.mongodb.util.TestCase {
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void dateParsing(){
        
        SimpleDateFormat format = new SimpleDateFormat(JSONCallback._msDateFormat);
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));

        Date rightNow = new Date();
        rightNow.setTime(System.currentTimeMillis());
        
        Date parsedDate = (Date)JSON.parse("{ \"$date\" : "+rightNow.getTime()+"}");
        assertEquals(rightNow.compareTo(parsedDate), 0);
        
        // Test formatted dates with ms granularity
        parsedDate = (Date)JSON.parse("{ \"$date\" : \""+format.format(rightNow)+"\"}");
        assertEquals(
                parsedDate.compareTo(format.parse(format.format(rightNow), new ParsePosition(0))), 0);
        
        // Test formatted dates with sec granularity
        format = new SimpleDateFormat(JSONCallback._secDateFormat);
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        
        parsedDate = (Date)JSON.parse("{ \"$date\" : \""+format.format(rightNow)+"\"}");
        assertEquals(
                parsedDate.compareTo(format.parse(format.format(rightNow), new ParsePosition(0))), 0);

    }

}
