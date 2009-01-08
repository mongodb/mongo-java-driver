// MongoTest.java

package com.mongodb;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.testng.annotations.Test;

import com.mongodb.util.*;

public class MongoTest extends TestCase {
    
    public MongoTest()
        throws IOException {
        _db = new Mongo( "127.0.0.1" , "mongotest" );        
    }
    
    final Mongo _db;
    
    public static void main( String args[] )
        throws Exception {
        (new MongoTest()).runConsole();
    }
    
}
