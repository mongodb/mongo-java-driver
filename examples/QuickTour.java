import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

import java.util.Set;
import java.util.List;

public class QuickTour {

    public static void main(String[] args) throws Exception {

        /*
         *   connect to the local database server for the 'mydb' database
         */
        Mongo db = new Mongo("127.0.0.1", "mydb");

        /*
         *  Authenticate - optional
         */
        // boolean auth = db.authenticate("foo", "bar");


        /*
         *  get a list of the collections in this database and print them out
         */
        Set<String> colls = db.getCollectionNames();

        for (String s : colls) {
            System.out.println(s);
        }

        /*
         *  get a collection object to work with
         */

        System.out.println(" --- ");

        DBCollection coll = db.getCollection("testCollection");

        /*
         *   drop all the data in it
         */

        coll.drop();


        /*
         *  make a document and insert it
         */

        BasicDBObject doc = new BasicDBObject();

        doc.put("name", "MongoDB");
        doc.put("type", "database");
        doc.put("count", 1);

        BasicDBObject info = new BasicDBObject();

        info.put("x", 203);
        info.put("y", 102);

        doc.put("info", info);

        coll.insert(doc);

        /*
         *  get it (since it's the only one in there since we dropped the rest earlier on)
         */
        DBObject myDoc = coll.findOne();

        System.out.println(myDoc);


        /*
         * now, lets add lots of little documents to the collection so we can explore queries and cursors
         */

        System.out.println(" --- ");

        for (int i=0; i < 100; i++) {
            coll.insert(new BasicDBObject().append("i", i));
        }

        System.out.println(coll.getCount());

        /*
         *  lets get all the documents in the collection and print them out
         */

        System.out.println(" --- ");

        DBCursor cur = coll.find();

        while(cur.hasNext()) {
            System.out.println(cur.next());
        }

        /*
         *  now use a query to get 1 document out
         */

        System.out.println(" --- ");
        
        BasicDBObject query = new BasicDBObject();

        query.put("i", 71);

        cur = coll.find(query);

        while(cur.hasNext()) {
            System.out.println(cur.next());
        }

        /*
         *  now use a query to get a larger set
         */

        System.out.println(" --- ");

        query = new BasicDBObject();

        query.put("i", new BasicDBObject("$gt", 50));  // i.e. find all where i > 50

        cur = coll.find(query);

        while(cur.hasNext()) {
            System.out.println(cur.next());
        }

        System.out.println(" --- ");

        query = new BasicDBObject();

        query.put("i", new BasicDBObject("$gt", 20).append("$lte", 30));  // i.e.   20 < i <= 30

        cur = coll.find(query);

        while(cur.hasNext()) {
            System.out.println(cur.next());
        }

        /*
         *  create an index on the "i" field
         */

       coll.createIndex(new BasicDBObject("i", 1));  // create index on "i", ascending


        /*
         *   list the indexes on the collection
         */

        List<DBObject> list = coll.getIndexInfo();

        for (DBObject o : list) {
            System.out.println(o);
        }
    }

}
