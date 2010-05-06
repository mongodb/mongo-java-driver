// BasicBSONCallback.java

package org.bson;

import java.io.*;
import java.util.*;

public class BasicBSONCallback implements BSONCallback {

    public BasicBSONCallback(){
        _root = new BasicBSONObject();
        _stack = new LinkedList<BSONObject>();
        _stack.add( _root );
    }
    
    public void objectStart(){
        if ( _stack.size() != 1 )
            throw new IllegalStateException( "something is wrong" );
    }
    
    public void objectStart(String name){
        BSONObject o = new BasicBSONObject();
        _stack.getLast().put( name , o );
        _stack.addLast( o );
    }
    
    public void objectDone(){
        _stack.removeLast();
    }

    
    final BSONObject _root;
    LinkedList<BSONObject> _stack;
}
