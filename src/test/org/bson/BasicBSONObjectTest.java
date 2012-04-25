package org.bson;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BasicBSONObjectTest extends Assert {

    @Test
    public void testEqualsWithMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "test", "test value" );
        BasicBSONObject bsonObject = new BasicBSONObject( map );
        assertTrue( bsonObject.equals( map ) );
    }

}
