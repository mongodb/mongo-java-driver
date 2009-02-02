
package com.mongodb.framework;

import java.util.*;
import java.net.*;

import com.mongodb.*;
import com.mongodb.util.*;


public class Find {

    public static void main(String[] args) 
        throws UnknownHostException {

        Mongo m = new Mongo( new DBAddress( "127.0.0.1:27017/driver_test_framework" ) );
        DBCollection c = m.getCollection( "test" );

        DBObject foo = new BasicDBObject();
        foo.put( "a", 2 );
        c.save( foo );
    }
}
