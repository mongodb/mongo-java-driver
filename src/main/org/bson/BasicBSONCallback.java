// BasicBSONCallback.java

package org.bson;

import java.io.*;
import java.util.*;

import org.bson.types.*;

public class BasicBSONCallback implements BSONCallback {

    public BasicBSONCallback(){
    }
    
    public BSONObject create(){
        return new BasicBSONObject();
    }

    public BSONObject create( boolean array , List<String> path ){
        if ( array )
            return new BasicBSONList();
        return new BasicBSONObject();
    }

    public void objectStart(){
        if ( _stack.size() > 0 )
            throw new IllegalStateException( "something is wrong" );
        _root = create();
        _stack.add( _root );
    }
    
    public void objectStart(String name){
        objectStart( false , name );
    }
    
    public void objectStart(boolean array, String name){
        _nameStack.addLast( name );
        BSONObject o = create( array , _nameStack );
        _stack.getLast().put( name , o );
        _stack.addLast( o );
    }
    
    public void objectDone(){
        _stack.removeLast();
        if ( _nameStack.size() > 0 )
            _nameStack.removeLast();
        else if ( _stack.size() > 0 )
            throw new IllegalStateException( "something is wrong" );
    }

    public void arrayStart(String name){
        objectStart( true , name );
    }

    public void arrayDone(){
        objectDone();
    }

    public void gotNull( String name ){
        cur().put( name , null );
    }
        
    public void gotUndefined( String name ){
    }

    public void gotMinKey( String name ){
        cur().put( name , "MinKey" );
    }
    public void gotMaxKey( String name ){
        cur().put( name , "MaxKey" );
    }
    
    public void gotBoolean( String name , boolean v ){
        cur().put( name , v );
    }
    
    public void gotDouble( String name , double v ){
        cur().put( name , v );
    }
    
    public void gotInt( String name , int v ){
        cur().put( name , v );
    }
    
    public void gotLong( String name , long v ){
        cur().put( name , v );
    }

    public void gotString( String name , String v ){
        cur().put( name , v );
    }
    public void gotSymbol( String name , String v ){
        cur().put( name , v );
    }

    public void gotTimestamp( String name , int time , int inc ){
        cur().put( name , new BSONTimestamp( time , inc ) );
    }

    
    BSONObject cur(){
        return _stack.getLast();
    }
    
    public BSONObject get(){
        return _root;
    }

    private BSONObject _root;
    private final LinkedList<BSONObject> _stack = new LinkedList<BSONObject>();
    private final LinkedList<String> _nameStack = new LinkedList<String>();
}
