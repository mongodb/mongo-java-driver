package com.xgen;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.mongodb.*;
import com.mongodb.util.JSON;

public class DriverTest {
    
    private Mongo connection = null;
    
    public Mongo getConnection() {
        return connection;
    }

    public DriverTest() throws UnknownHostException, MongoException {
        
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        addrs.add( new ServerAddress( "127.0.0.1" , 27017 ) );
        addrs.add( new ServerAddress( "127.0.0.1" , 27018 ) );
        addrs.add( new ServerAddress( "127.0.0.1" , 27019 ) );

        connection = new Mongo(addrs);
        
        
        Set<String> names = new HashSet<String>();
        names.add("primary");
        LinkedHashMap<String, String> tagSet1 = new LinkedHashMap<String, String>();
        tagSet1.put("foo", "1");
        tagSet1.put("bar", "2");
        tagSet1.put("baz", "1");
        
        LinkedHashMap<String, String> tagSet2 = new LinkedHashMap<String, String>();
        tagSet2.put("foo", "1");
        tagSet2.put("bar", "2");
        tagSet2.put("baz", "2");
        
        LinkedHashMap<String, String> tagSet3 = new LinkedHashMap<String, String>();
        tagSet3.put("foo", "1");
        tagSet3.put("bar", "2");
        tagSet3.put("baz", "3");
        
    }
    
    public static void main(String[] args){
                
        DriverTest instance = null;
        try {
            instance = new DriverTest();
            DB db = instance.getConnection().getDB("test");

            DBCollection coll = db.getCollection("test");
            DBObject query = (DBObject)JSON.parse("{ \"me\" : \"Tarzan\" }");
            
             Set<String> names = new HashSet<String>();
             names.add("primary");
             LinkedHashMap<String, String> tagSet1 = new LinkedHashMap<String, String>();
             tagSet1.put("foo", "1");
             ReadPreference preference = new ReadPreference.PrimaryPreferredReadPreference(new BasicDBObject("foo", "1"));
             DBCursor cur = new DBCursor(coll, query, null, preference);
             
             
             if(cur.hasNext())
                 System.out.println(cur.next());
             
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MongoException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }   
    }
    
    LinkedHashMap<String, String> tagSet1, tagSet2, tagSet3;
}
