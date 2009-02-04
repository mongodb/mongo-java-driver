
package com.mongodb.framework;

import java.util.*;
import java.net.*;

import com.mongodb.*;
import com.mongodb.util.*;

public class Stress1 {

    public static void doStuff( DBCollection c, int count ) {
        DBObject obj = new BasicDBObject();
        obj.put( "id", count );
        DBObject x = c.findOne( obj );
        x.put( "subarray", "foo" + count );
        c.save( x );
    }

    public static void main(String[] args) 
        throws UnknownHostException {

        Mongo m = new Mongo( new DBAddress( "127.0.0.1:27017/driver_test_framework" ) );
        DBCollection c = m.getCollection( "stress1" );

        String blah = "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas" + 
            "lksjhasoh1298alshasoidiohaskjasiouashoasasiugoas";

        for( int i=0; i<50000; i++ ) {
            DBObject foo = new BasicDBObject();
            foo.put( "name", "asdf"+i );
            foo.put( "date", new Date() );
            foo.put( "id", i );
            foo.put( "blah", blah );
            c.save( foo );
        }

        for( int count=0; count<10000; count++ ) {
            doStuff( c, count );
        }

        DBObject idx = new BasicDBObject();
        idx.put( "date", 1 );
        c.ensureIndex( idx );
    }
}
