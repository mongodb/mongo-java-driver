package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.util.Arrays;

public class ReadPreferenceTest extends TestCase {
    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecatedStaticMembers() {
        assertSame(ReadPreference.primary(), ReadPreference.PRIMARY);
        assertSame(ReadPreference.secondaryPreferred(), ReadPreference.SECONDARY);
    }

    @Test
    public void testToString() {
        assertEquals("{ \"mode\" : \"primary\"}", ReadPreference.primary().toDBObject().toString());
        assertEquals("{ \"mode\" : \"secondaryPreferred\"}", ReadPreference.secondaryPreferred().toDBObject().toString());
        assertEquals("{ \"mode\" : \"nearest\"}", ReadPreference.nearest().toDBObject().toString());
    }

    @Test
    public void testSecondaryReadPreference() {
        final BasicDBObject asDBObject = new BasicDBObject("mode", "secondary");
        assertEquals(asDBObject, ReadPreference.secondary().toDBObject());

        assertEquals(asDBObject.append("tags", Arrays.asList(new BasicDBObject("tag", "1"))),
                ReadPreference.secondary(new BasicDBObject("tag", "1")).toDBObject());
    }

    @Test
    public void testPrimaryPreferredMode() {
        final BasicDBObject asDBObject = new BasicDBObject("mode", "primaryPreferred");
        assertEquals(asDBObject, ReadPreference.primaryPreferred().toDBObject());

        assertEquals(asDBObject.append("tags", Arrays.asList(new BasicDBObject("tag", "1"))),
                ReadPreference.primaryPreferred(new BasicDBObject("tag", "1")).toDBObject());
    }

    @Test
    public void testSecondaryPreferredMode() {
        final BasicDBObject asDBObject = new BasicDBObject("mode", "secondaryPreferred");
        assertEquals(asDBObject, ReadPreference.secondaryPreferred().toDBObject());

        assertEquals(asDBObject.append("tags", Arrays.asList(new BasicDBObject("tag", "1"))),
                ReadPreference.secondaryPreferred(new BasicDBObject("tag", "1")).toDBObject());

    }

    @Test
    public void testNearestMode() {
        final BasicDBObject asDBObject = new BasicDBObject("mode", "nearest");
        assertEquals(asDBObject, ReadPreference.nearest().toDBObject());

        assertEquals(asDBObject.append("tags", Arrays.asList(new BasicDBObject("tag", "1"))),
                ReadPreference.nearest(new BasicDBObject("tag", "1")).toDBObject());

    }

    @Test
    public void testValueOf() {
        assertEquals(ReadPreference.primary(), ReadPreference.valueOf("primary"));
        assertEquals(ReadPreference.secondary(), ReadPreference.valueOf("secondary"));
        assertEquals(ReadPreference.primaryPreferred(), ReadPreference.valueOf("primaryPreferred"));
        assertEquals(ReadPreference.secondaryPreferred(), ReadPreference.valueOf("secondaryPreferred"));
        assertEquals(ReadPreference.nearest(), ReadPreference.valueOf("nearest"));

        DBObject first = new BasicDBObject("dy", "ny");
        DBObject remaining = new BasicDBObject();
        assertEquals(ReadPreference.secondary(first, remaining), ReadPreference.valueOf("secondary", first, remaining));
        assertEquals(ReadPreference.primaryPreferred(first, remaining), ReadPreference.valueOf("primaryPreferred", first, remaining));
        assertEquals(ReadPreference.secondaryPreferred(first, remaining), ReadPreference.valueOf("secondaryPreferred", first, remaining));
        assertEquals(ReadPreference.nearest(first, remaining), ReadPreference.valueOf("nearest", first, remaining));
    }

    @Test
    public void testGetName() {
        assertEquals("primary", ReadPreference.primary());
        assertEquals("secondary", ReadPreference.secondary());
        assertEquals("primaryPreferred", ReadPreference.primaryPreferred());
        assertEquals("secondaryPreferred", ReadPreference.secondaryPreferred());
        assertEquals("nearest", ReadPreference.nearest());

        DBObject first = new BasicDBObject("dy", "ny");
        DBObject remaining = new BasicDBObject();
        assertEquals(ReadPreference.secondary(first, remaining), ReadPreference.valueOf("secondary", first, remaining));
        assertEquals(ReadPreference.primaryPreferred(first, remaining), ReadPreference.valueOf("primaryPreferred", first, remaining));
        assertEquals(ReadPreference.secondaryPreferred(first, remaining), ReadPreference.valueOf("secondaryPreferred", first, remaining));
        assertEquals(ReadPreference.nearest(first, remaining), ReadPreference.valueOf("nearest", first, remaining));
    }
}
