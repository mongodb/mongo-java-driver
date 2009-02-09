import com.mongodb.MongoAdmin;
import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;

public class QuickTourAdmin {
    
    public static void main(String[] args) throws Exception {

        /*
         *   connect to the local database server 
         */
        MongoAdmin admin = new MongoAdmin();

        /*
         *  Authenticate - optional
         */
        // boolean auth = db.authenticate("foo", "bar");

        for (String s : admin.getDatabaseNames()) {
            System.out.println(s);
        }


        /*
         *   get a db
         */

        Mongo m = admin.getDatabase("com_mongodb_MongoAdmin");

        /*
         * do an insert so that the db will really be created.  Calling getDB() doesn't really take any
         * action with the server 
         */
        m.getCollection("testcollection").insert(new BasicDBObject("i",1));
        
        for (String s : admin.getDatabaseNames()) {
            System.out.println(s);
        }

        /*
         *   drop a database
         */

        admin.dropDatabase("com_mongodb_MongoAdmin");

        for (String s : admin.getDatabaseNames()) {
            System.out.println(s);
        }        
    }
}
