
package com.mongodb.framework;

import java.util.*;
import java.net.*;

import com.mongodb.*;
import com.mongodb.util.*;

public class Dbs {

    public static void main(String[] args) 
        throws UnknownHostException {

        Mongo m = new Mongo( new DBAddress( "127.0.0.1:27017/driver_test_framework" ) );
        DBCollection coll = m.getCollection( "dbs_1" );
        DBObject o = new BasicDBObject();
        o.put( "foo", "bar" );
        coll.save( o );

        coll = m.getCollection( "dbs_2" );
        o = new BasicDBObject();
        o.put( "psi", "phi" );
        coll.save( o );

        System.out.println( m.getName() );
        Set<String> set = m.getCollectionNames();
        String[] strs = set.toArray( new String[0] );
        Arrays.sort( strs );
        for( String s : strs ) {
            if( s.startsWith( "dbs" ) ) {
                System.out.println( s );
            }
        }

        m.getCollection( "dbs_1" ).drop();
        o = new BasicDBObject();
        o.put( "create", "dbs_3" );
        m.command( o );

        set = m.getCollectionNames();
        strs = set.toArray( new String[0] );
        Arrays.sort( strs );
        for( String s : strs ) {
            if( s.startsWith( "dbs" ) ) {
                System.out.println( s );
            }
        }
    }
}
