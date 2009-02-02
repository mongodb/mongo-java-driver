
package com.mongodb.framework;

import java.util.*;
import java.net.*;

import com.mongodb.*;
import com.mongodb.util.*;


public class Count1 {

    public static void main(String[] args) 
        throws UnknownHostException {

        Mongo m = new Mongo( new DBAddress( "127.0.0.1:27017/driver_test_framework" ) );
        System.out.println( m.getCollection( "test1" ).find().count() );
        System.out.println( m.getCollection( "test2" ).find().count() );
        DBCollection coll = m.getCollection( "test3" );

        DBObject foo = new BasicDBObject();
        foo.put( "i", "a" );
        System.out.println( coll.find( foo ).count() );

        foo.put( "i", 3 );
        System.out.println( coll.find( foo ).count() );

        DBObject bar = new BasicDBObject();
        bar.put( "$gte" , 67 );
        foo.put( "i", bar );
        System.out.println( coll.find( foo ).count() );
    }
}
