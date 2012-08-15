package org.bson;

import org.bson.types.BSONTimestamp;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BSONTimestampTest extends Assert{

    @Test
    public void testComparable(){
        
        int currTime = (int)(System.currentTimeMillis() / 1000);
        
        BSONTimestamp t1 = new BSONTimestamp(currTime, 1);
        BSONTimestamp t2 = new BSONTimestamp(currTime, 1);
        
        assertEquals(0, t1.compareTo(t2));
        
        t2 = new BSONTimestamp(currTime,  2);
        
        assertTrue(t1.compareTo(t2) < 0);
        assertTrue(t2.compareTo(t1) > 0);
        
        t2 = new BSONTimestamp(currTime + 1, 1);
        
        assertTrue(t1.compareTo(t2) < 0);
        assertTrue(t2.compareTo(t1) > 0);
    }
}
