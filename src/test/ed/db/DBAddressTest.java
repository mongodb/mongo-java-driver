// DBAddressTest.java

package ed.db;

import java.net.*;

import org.testng.annotations.Test;

import ed.*;

public class DBAddressTest extends TestCase {

    @Test(groups = {"basic"})
    public void testCTOR() 
        throws UnknownHostException {
        DBAddress foo = new DBAddress( "www.10gen.com:1000/some.host" );
        DBAddress bar = new DBAddress( foo, "some.other.host" );
        assertEquals( foo.sameHost( "www.10gen.com:1000" ), true );
        assertEquals( foo.getSocketAddress().hashCode(), bar.getSocketAddress().hashCode() );
        assertEquals( foo.toString(), "www.10gen.com:1000/some-host" );
    }

    @Test(groups = {"basic"})
    public void testInvalid() 
        throws UnknownHostException {
        boolean threw = false;
        try { 
            new DBAddress( null );
        }
        catch( NullPointerException e ) {
            threw = true;
        }
        assertTrue( threw, "new DBAddress(null) didn't throw exception" );
        threw = false;

        try { 
            new DBAddress( "  \t\n" );
        }
        catch( IllegalArgumentException e ) {
            threw = true;
        }
        assertTrue( threw, "new DBAddress(\" \") didn't throw exception" );
        threw = false;
    }

    public static void main( String args[] ) {
        (new DBAddressTest()).runConsole();
    }
}

