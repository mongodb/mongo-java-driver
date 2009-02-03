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
        DBCollection b = m.getCollection( "b" );
        foo.put( "c", b );
        m.getCollection( "a" ).save( foo );

        foo = new BasicDBObject();
        foo.put( "c", 1 );
        b.save( foo );

        ObjectId id = new ObjectId();
        foo = new BasicDBObject();
        foo.put( "_id", id );
        foo.put( "that", 2 );
        DBRef ref = new DBRef( "c", id );
        foo.put( "thiz", ref );
        m.getCollection( "c" ).save( foo );

    }
}
