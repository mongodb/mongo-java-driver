package com.mongodb.framework;

import java.util.*;
import java.net.*;

import com.mongodb.*;
import com.mongodb.util.*;


public class Circular {

    public static void main(String[] args) 
        throws UnknownHostException {

        Mongo m = new Mongo( new DBAddress( "127.0.0.1:27017/driver_test_framework" ) );
        DBObject foo = new BasicDBObject();
        m.getCollection( "remove1" ).remove( foo );

        foo.put( "a", 3 );
        m.getCollection( "remove1" ).remove( foo );
    }
}
