// DBMessageLayer.java

package ed.db;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;

public abstract class DBMessageLayer extends DBApiLayer {
    
    DBMessageLayer( String root ){
        super( root );
    }

    protected void doInsert( ByteBuffer buf ){
        say( 2002 , buf );
    }
    protected  void doDelete( ByteBuffer buf ){
        say( 2006 , buf );
    }
    protected void doUpdate( ByteBuffer buf ){
        say( 2001 , buf );
    }
    protected void doKillCursors( ByteBuffer buf ){
        say( 2007 , buf );
    }
    
    protected int doQuery( ByteBuffer out , ByteBuffer in ){
        return call( 2004 , out , in );
    }
    protected int doGetMore( ByteBuffer out , ByteBuffer in ){
        return call( 2005 , out , in );
    }
    
    protected abstract void say( int op , ByteBuffer buf );
    protected abstract int call( int op , ByteBuffer out , ByteBuffer in );

}
