// Mongo.java

package ed.db;

import java.net.*;

public class Mongo extends DBTCP {

    public Mongo( String host , String dbName )
        throws UnknownHostException {
        super( new DBAddress( host , dbName ) );
    }

    public Mongo( String host , int port , String dbName )
        throws UnknownHostException {
        super( new DBAddress( host , port , dbName ) );
    }

    public Mongo( DBAddress addr ){
        super( addr );
    }
}
