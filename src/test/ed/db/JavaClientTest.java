// JavaClientTest.java

package ed.db;

import java.io.*;
import java.util.*;

import ed.*;

public class JavaClientTest extends TestCase {
    
    JavaClientTest()
        throws IOException {
        _db = new Mongo( "127.0.0.1" , "jtest" );        
    }
    
    public void test1(){
        DBCollection c = _db.getCollection( "test1" );;

        DBObject m = new BasicDBObject();
        m.put( "name" , "eliot" );
        m.put( "state" , "ny" );
        
        c.save( m );
        
        assert( m.containsKey( "_id" ) );

        Map out = (Map)(c.find( m.get( "_id" ).toString() ));
        assertEquals( "eliot" , out.get( "name" ) );
        assertEquals( "ny" , out.get( "state" ) );
    }

    public void test2(){
        DBCollection c = _db.getCollection( "test2" );;
        
        DBObject m = new BasicDBObject();
        m.put( "name" , "eliot" );
        m.put( "state" , "ny" );
        
        Map<String,Object> sub = new HashMap<String,Object>();
        sub.put( "bar" , "1z" );
        m.put( "foo" , sub );

        c.save( m );
        
        assert( m.containsKey( "_id" ) );

        Map out = (Map)(c.find( m.get( "_id" ).toString() ));
        assertEquals( "eliot" , out.get( "name" ) );
        assertEquals( "ny" , out.get( "state" ) );

        Map z = (Map)out.get( "foo" );
        assertNotNull( z );
        assertEquals( "1z" , z.get( "bar" ) );
    }

    final Mongo _db;

    public static void main( String args[] )
        throws Exception {
        (new JavaClientTest()).runConsole();
    }
}
