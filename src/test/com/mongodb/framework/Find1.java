
package com.mongodb.framework;

import java.util.*;
import java.net.*;

import com.mongodb.*;
import com.mongodb.util.*;


public class Find1 {

    public static void main(String[] args) 
        throws UnknownHostException {

        Mongo m = new Mongo( new DBAddress( "127.0.0.1:27017/driver_test_framework" ) );
        DBObject foo = new BasicDBObject();
        foo.put( "x", 1 );
        DBObject bar = new BasicDBObject();
        bar.put( "y", 1 );
        DBCursor cursor = m.getCollection( "c" ).find( foo ).sort( bar ).skip( 20 ).limit( 10 );

        while( cursor.hasNext() ) {
            System.out.println( cursor.next().get( "z" ) );
        }
    }
}
