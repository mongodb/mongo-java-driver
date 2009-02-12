package com.mongodb;

import org.testng.annotations.Test;

/**
 *  Tests aspect of the DB - not really driver tests
 */
public class DBTests {

    /**
     *   This test will fail now.  Waiting for fix in DB.  Originally done to show the
     *   effect of removing "seen" in DBApiLayer
     * 
     * @throws Exception
     */
    @Test
    public void testConcurrentUpdate()  throws Exception {

        Mongo m = new Mongo("com_mongodb_DBTests");

        DBCollection coll = m.getCollection("concurrentUpdate");

        coll.drop();

        for (int i = 0; i < 10000; i++) {
            coll.insert(new BasicDBObject("i", i).append("flarg", "ASDASDASDASDASDA)DA_)ASD_)IASD_OJASD_)AS_D)IAS_D)IAS_D)IA_S)DIA_S)DIA_S)DIA_S)DIA_S)DIA_S)DIA_S)DIA_S)DIA_S)DIA_S)DIA_S)"));
        }
        int count = 0;

        DBCursor c = coll.find();

        while(c.hasNext()) {

            DBObject o = c.next();

            if (count++ == 0) {
                o.put("woog", "12-3012-30123-0123-012312312-0312-012-01-203-012-0psaoija-0sid-as0daoisasoidas=0dias=0diapsodjasd");
                coll.update(new BasicDBObject("_id", o.get("_id")), o, false, false);
            }
        }

        System.out.println(count);
        assert(count == 10000);
    }


}
