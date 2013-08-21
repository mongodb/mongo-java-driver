package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.*;

/**
 * Test that QueryOpBuilder creates query operations that abide by
 *
 * @author stevebriskin
 */
public class QueryOpTest extends TestCase {

    @Test
    public void testQueryOnly() {
        DBObject query = QueryBuilder.start("x").greaterThan(1).get();
        DBObject obj = new QueryOpBuilder().addQuery(QueryBuilder.start("x").greaterThan(1).get()).get();
        assertEquals(query, obj);

        assertNotNull(new QueryOpBuilder().get());

    }

    @Test
    public void testQueryAndOthers() {
        DBObject query = QueryBuilder.start("x").greaterThan(1).get();
        DBObject orderBy = new BasicDBObject("x", 1);
        DBObject hintObj = new BasicDBObject("x_i", 1);
        String hintStr = "y_i";


        DBObject queryOp = new QueryOpBuilder().addQuery(query).addOrderBy(orderBy).addHint(hintStr).get();
        assertEquals(queryOp.get("$query"), query);
        assertEquals(queryOp.get("$orderby"), orderBy);
        assertEquals(queryOp.get("$hint"), hintStr);
        assertNull(queryOp.get("$explain"));
        assertNull(queryOp.get("$snapshot"));

        //orderby should only be there if added
        queryOp = new QueryOpBuilder().addQuery(query).addHint(hintStr).get();
        assertEquals(queryOp.get("$query"), query);
        assertNull(queryOp.get("$orderby"));

        //hintObj takes precedence over hintStr
        queryOp = new QueryOpBuilder().addQuery(query).addOrderBy(orderBy).addHint(hintStr).addHint(hintObj).get();
        assertEquals(queryOp.get("$query"), query);
        assertEquals(queryOp.get("$orderby"), orderBy);
        assertEquals(queryOp.get("$hint"), hintObj);

        queryOp = new QueryOpBuilder().addQuery(query).addExplain(true).addSnapshot(true).get();
        assertEquals(queryOp.get("$query"), query);
        assertNull(queryOp.get("$orderby"));
        assertNull(queryOp.get("$hint"));
        assertEquals(queryOp.get("$explain"), true);
        assertEquals(queryOp.get("$snapshot"), true);

        queryOp = new QueryOpBuilder().addQuery(query).addSpecialFields(new BasicDBObject("flag", "val")).get();
        assertEquals(queryOp.get("flag"), "val");
        assertEquals(queryOp.get("$query"), query);
        assertNull(queryOp.get("$orderby"));
        assertNull(queryOp.get("$hint"));
        assertNull(queryOp.get("$explain"));
        assertNull(queryOp.get("$snapshot"));


        // only append $readPreference if the read preference is not ReadPreference.primary()

        queryOp = new QueryOpBuilder().addQuery(query).addReadPreference(ReadPreference.primary()).get();
        assertEquals(queryOp.get("$query"), query);
        assertNull(queryOp.get("$orderby"));
        assertNull(queryOp.get("$hint"));
        assertNull(queryOp.get("$explain"));
        assertNull(queryOp.get("$snapshot"));
        assertNull(queryOp.get("$readPreference"));

        queryOp = new QueryOpBuilder().addQuery(query).addReadPreference(ReadPreference.secondary()).get();
        assertEquals(queryOp.get("$query"), query);
        assertNull(queryOp.get("$orderby"));
        assertNull(queryOp.get("$hint"));
        assertNull(queryOp.get("$explain"));
        assertNull(queryOp.get("$snapshot"));
        assertEquals(ReadPreference.secondary().toDBObject(), queryOp.get("$readPreference"));
    }
}
