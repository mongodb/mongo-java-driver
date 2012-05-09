package com.mongodb.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

public class JSONCallbackTest extends com.mongodb.util.TestCase {
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void dateParsing(){
        long currentTime = System.currentTimeMillis();
        Date rightNow = new Date();
        rightNow.setTime(currentTime);
        
        SimpleDateFormat format =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        
        Date parsedDate = (Date)JSON.parse("{ \"$date\" : "+rightNow.getTime()/1000l+"}");
        assertEquals(format.format(rightNow), format.format(parsedDate));
        
        parsedDate = (Date)JSON.parse("{ \"$date\" : \""+rightNow.getTime()/1000l+"\"}");
        assertEquals(format.format(rightNow), format.format(parsedDate));
        
        parsedDate = (Date)JSON.parse("{ \"$date\" : \""+format.format(rightNow)+"\"}");
        assertEquals(format.format(rightNow), format.format(parsedDate));
    }

}
