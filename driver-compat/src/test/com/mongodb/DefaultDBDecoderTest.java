package com.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultDBDecoderTest extends DatabaseTestCase {

    @Test
    public void testDecodingDBRef() {
        final DBObject dbObject = new BasicDBObject("r", new DBRef(database, "test", 1));
        final byte[] bytes = {37, 0, 0, 0, 3, 114, 0, 29, 0, 0, 0, 2, 36, 114, 101, 102, 0, 5, 0, 0, 0, 116, 101, 115, 116, 0, 16, 36, 105, 100, 0, 1, 0, 0, 0, 0, 0};
        DBObject o = new DefaultDBDecoder().decode(bytes, collection);
        assertEquals(dbObject, o);
    }

    @Test
    public void testTypeMapping() {
        collection.setObjectClass(MyDBObject.class);
        collection.setInternalClass("a", AnotherDBObject.class);
        final byte[] bytes = {20, 0, 0, 0, 3, 97, 0, 12, 0, 0, 0, 16, 105, 0, 1, 0, 0, 0, 0, 0};
        DBObject object = new DefaultDBDecoder().decode(bytes, collection);
        assertEquals(MyDBObject.class, object.getClass());
        assertEquals(AnotherDBObject.class, object.get("a").getClass());
    }

    @SuppressWarnings("serial")
    public static class MyDBObject extends BasicDBObject {

    }

    @SuppressWarnings("serial")
    public static class AnotherDBObject extends BasicDBObject {

    }
}
