// DBMessage.java

package ed.db;

import java.nio.*;

public class DBMessage {
    
    static int ID = 1;
    static int HEADER_LENGTH = 16;

    DBMessage( int operation , ByteBuffer data ){
        _id = ID++;
        _responseTo = 0;
        _operation = operation;
        _data = data;
        
        if ( _data.position() > 0 )
            _data.flip();
        
        _len = HEADER_LENGTH + data.limit();
    }
    
    DBMessage( ByteBuffer buf , ByteBuffer dataBuffer ){
        _len = buf.getInt();
        _id = buf.getInt();
        _responseTo = buf.getInt();
        _operation = buf.getInt();

        _data = dataBuffer;
    }

    void putHeader( ByteBuffer buf ){
        buf.putInt( _len );
        buf.putInt( _id ) ;
        buf.putInt( _responseTo );
        buf.putInt( _operation );
    }

    ByteBuffer getData(){
        return _data;
    }

    int dataLen(){
        return _len - HEADER_LENGTH;
    }

    final int _len;    
    final int _id;
    final int _responseTo;
    final int _operation;

    
    final ByteBuffer _data;
}
