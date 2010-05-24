// BasicBSONCallback.java

package org.bson;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.bson.types.*;

public class BasicBSONCallback implements BSONCallback {

    public BasicBSONCallback(){
        reset();
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
        if ( _stack.size() > 0 ) {
	    throw new IllegalStateException( "something is wrong" );
	}
	objectStart(false);
    }

    public void objectStart(boolean array){
        _root = create(array, null);
        _stack.add( (BSONObject)_root );
    }
    
    public void objectStart(String name){
        objectStart( false , name );
    }
    
    public void objectStart(boolean array, String name){
        _nameStack.addLast( name );
        BSONObject o = create( array , _nameStack );
        _stack.getLast().put( name , o);
        _stack.addLast( o );
    }
    
    public Object objectDone(){
        BSONObject o =_stack.removeLast();
        if ( _nameStack.size() > 0 )
            _nameStack.removeLast();
        else if ( _stack.size() > 0 ) {
	    throw new IllegalStateException( "something is wrong" );
	}
        return (BSONObject)BSON.applyDecodingHooks(o);
    }

    public void arrayStart(){
	objectStart( true );
    }

    public void arrayStart(String name){
        objectStart( true , name );
    }

    public Object arrayDone(){
        return objectDone();
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
        _put( name , v );
    }
    
    public void gotDouble( String name , double v ){
        _put( name , v );
    }
    
    public void gotInt( String name , int v ){
        _put( name , v );
    }
    
    public void gotLong( String name , long v ){
        _put( name , v );
    }

    public void gotDate( String name , long millis ){
        _put( name , new Date( millis ) );
    }
    public void gotRegex( String name , String pattern , String flags ){
        _put( name , Pattern.compile( pattern , BSON.regexFlags( flags ) ) );
    }
    
    public void gotString( String name , String v ){
        _put( name , v );
    }
    public void gotSymbol( String name , String v ){
        _put( name , v );
    }

    public void gotTimestamp( String name , int time , int inc ){
        _put( name , new BSONTimestamp( time , inc ) );
    }
    public void gotObjectId( String name , ObjectId id ){
        _put( name , id );
    }
    public void gotDBRef( String name , String ns , ObjectId id ){
        _put( name , new BasicBSONObject( "$ns" , ns ).append( "$id" , id ) );
    }

    public void gotBinaryArray( String name , byte[] b ){
        _put( name , b );
    }
    
    public void gotBinary( String name , byte type , byte[] data ){
        _put( name , new Binary( type , data ) );
    }

    protected void _put( String name , Object o ){
        cur().put( name , BSON.applyDecodingHooks( o ) );
    }
    
    protected BSONObject cur(){
        return _stack.getLast();
    }
    
    public Object get(){
	return _root;
    }

    protected void setRoot(Object o) {
	_root = o;
    }

    protected boolean isStackEmpty() {
	return _stack.size() < 1;
    }    
    
    public void reset(){
        _root = null;
        _stack.clear();
        _nameStack.clear();
    }

    private Object _root;
    private final LinkedList<BSONObject> _stack = new LinkedList<BSONObject>();
    private final LinkedList<String> _nameStack = new LinkedList<String>();
}
