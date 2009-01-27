// ByteEncoder.java

package com.mongodb;

import java.util.*;
import java.util.regex.*;
import java.nio.*;
import java.nio.charset.*;
import java.lang.reflect.Array;

import com.mongodb.util.*;

public class ByteEncoder extends Bytes {

    static final boolean DEBUG = Boolean.getBoolean( "DEBUG.BE" );

    // things that won't get sent in the scope
    static final Set<String> BAD_GLOBALS = new HashSet<String>(); 
    static {
	BAD_GLOBALS.add( "db" );
	BAD_GLOBALS.add( "local" );
	BAD_GLOBALS.add( "core" );
        BAD_GLOBALS.add( "args" ); // TODO: should we get rid of this
        BAD_GLOBALS.add( "obj" ); // TODO: get rid of this
    }
    
    
    public static boolean dbOnlyField( Object o ){
        if ( o == null )
            return false;
        
        if ( o instanceof String )
            return dbOnlyField( o.toString() );
        
        return false;
    }

    public static boolean dbOnlyField( String s ){
        return 
            s.equals( "_ns" )  
            || s.equals( "_save" )
            || s.equals( "_update" );
    }

    public static ByteEncoder get(){
        return _pool.get();
    }

    protected void done(){
        reset();
        _pool.done( this );
    }
    
    private final static int _poolSize = Math.min( Bytes.CONNECTIONS_PER_HOST , 2 * BUFS_PER_50M );
    private final static SimplePool<ByteEncoder> _pool = new SimplePool<ByteEncoder>( "ByteEncoders" , _poolSize , -1 ){
            protected ByteEncoder createNew(){
		if ( D ) System.out.println( "creating new ByteEncoder" );
                return new ByteEncoder();
            }

            protected long memSize( ByteEncoder d ){
                return BUF_SIZE + ( 2 * MAX_STRING ) + 1024;
            }
        };

    public ByteEncoder( ByteBuffer buf ){
        _buf = buf;
        _buf.order( Bytes.ORDER );        
    }

    // ----
    
    private ByteEncoder(){
        _buf = ByteBuffer.allocateDirect( BUF_SIZE );
        _buf.order( Bytes.ORDER );
    }

    /**
     *  Returns the bytes in the bytebuffer.  Attempts to leave the
     *  bytebuffer in the same state.  Note that mark, if set, is lost.
     *
     * @return  array of bytes
     */
    public byte[] getBytes() {

        int pos = _buf.position();
        int limit = _buf.limit();

        flip();

        byte[] arr = new byte[_buf.limit()];

        _buf.get(arr);

        flip();

        _buf.position(pos);
        _buf.limit(limit);

        return arr;
    }

    protected void reset(){
        _buf.position( 0 );
        _buf.limit( _buf.capacity() );
        _flipped = false;
	_dontRef.clear();
    }

    protected void flip(){
        _buf.flip();
        _flipped = true;
    }
    
    /**
     * this is for the higher level api calls
     */
    public int putObject( DBObject o ){
        try {
            return putObject( null , o );
        }
        catch ( BufferOverflowException bof ){
            reset();
            throw new RuntimeException( "tried to save too large of an object.  max size : " + ( _buf.capacity() / 2  ) );
        }
    }

    /**
     * this is really for embedded objects
     */
    private int putObject( String name , DBObject o ){
        if ( o == null )
            throw new NullPointerException( "can't save a null object" );

        if ( DEBUG ) System.out.println( "putObject : " + name + " [" + o.getClass() + "]" + " # keys " + o.keySet().size() );
        
        if ( _flipped )
            throw new RuntimeException( "already flipped" );
        final int start = _buf.position();
        
        byte myType = OBJECT;
        if ( o instanceof List )
            myType = ARRAY;

        if ( _handleSpecialObjects( name , o ) )
            return _buf.position() - start;
        
        if ( name != null ){
            _put( myType , name );
        }

        final int sizePos = _buf.position();
        _buf.putInt( 0 ); // leaving space for this.  set it at the end

        if ( o.containsKey( "_id" ) )
            _putObjectField( "_id" , o.get( "_id" ) );
            
        List transientFields = null;
        {
            Object temp = o.get( "_transientFields" );
            if ( temp instanceof List )
                transientFields = (List)temp;
        }
        

        for ( String s : o.keySet() ){

            if ( s.equals( "_id" ) )
                continue;
            
            if ( transientFields != null && transientFields.contains( s ) )
                continue;
            
            Object val = o.get( s );

            _putObjectField( s , val );

        }
        _buf.put( EOO );
        
        _buf.putInt( sizePos , _buf.position() - sizePos );
        return _buf.position() - start;
    }

    private void _putObjectField( String name , Object val ){

        if ( dbOnlyField( name ) || name.equals( "_transientFields" ) )
            return;
        
        if ( DEBUG ) System.out.println( "\t put thing : " + name );
        
        if ( name.equals( "$where") && val instanceof String ){
            _put( CODE , name );
            _putValueString( val.toString() );
            return;
        }
        
        if ( val == null )
            putNull(name);
        else if ( val instanceof Date )
            putDate( name , (Date)val );
        else if ( val instanceof Number )
            putNumber(name, (Number)val );
        else if ( val instanceof String )
            putString(name, val.toString() );
        else if ( val instanceof ObjectId )
            putObjectId(name, (ObjectId)val );
        else if ( val instanceof DBObject )
            putObject(name, (DBObject)val );
        else if ( val instanceof Boolean )
            putBoolean(name, (Boolean)val );
        else if ( val instanceof Pattern )
            putPattern(name, (Pattern)val );
        else if (val instanceof DBRegex) {
            putDBRegex(name, (DBRegex) val);
        }
        else if ( val instanceof Map )
            putMap( name , (Map)val );
        else if ( val instanceof List )
            putList( name , (List)val );
        else if ( val instanceof byte[] )
            putBinary( name , (byte[])val );
        else if (val instanceof DBRef) {

            // temporary - there's the notion of "special object" , but for simple level 0...
            DBRef r = (DBRef) val;
            putDBRef( name , r._ns , r._id );
        }
        else if (val instanceof DBSymbol) {
            putSymbol(name, (DBSymbol) val);
        }
        else if (val instanceof DBUndefined) {
            putUndefined(name);
        }
        else 
            throw new RuntimeException( "can't serialize " + val.getClass() );
        
    }

    private void putList( String name , List l ){
        _put( ARRAY , name );
        final int sizePos = _buf.position();
        _buf.putInt( 0 );
        
        for ( int i=0; i<l.size(); i++ )
            _putObjectField( String.valueOf( i ) , l.get( i ) );

        _buf.put( EOO );
        _buf.putInt( sizePos , _buf.position() - sizePos );        
    }
    
    private void putMap( String name , Map m ){
        _put( OBJECT , name );
        final int sizePos = _buf.position();
        _buf.putInt( 0 );
        
        for ( Object key : m.keySet() )
            _putObjectField( key.toString() , m.get( key ) );

        _buf.put( EOO );
        _buf.putInt( sizePos , _buf.position() - sizePos );
    }
    

    private boolean _handleSpecialObjects( String name , DBObject o ){
        
        if ( o == null )
            return false;

        if ( o instanceof DBCollection ){
            DBCollection c = (DBCollection)o;
            putDBRef( name , c.getName() , Bytes.COLLECTION_REF_ID );
            return true;
        }
        
        if ( ! _dontRefContains( o ) && name != null && o instanceof DBRef ){
            DBRef r = (DBRef)o;
            putDBRef( name , r._ns , r._id );
            return true;
        }
        
        if ( o.get( Bytes.NO_REF_HACK ) != null ){
            o.removeField( Bytes.NO_REF_HACK );
            return false;
        }

        if ( ! _dontRefContains( o ) && 
	     name != null && 
             cameFromDB( o ) ){
            putDBRef( name , o.get( "_ns" ).toString() , (ObjectId)(o.get( "_id" ) ) );
            return true;
        }
        
        return false;
    }

    protected int putNull( String name ){
        int start = _buf.position();
        _put( NULL , name );
        return _buf.position() - start;
    }

    protected int putUndefined(String name){
        int start = _buf.position();
        _put(UNDEFINED, name);
        return _buf.position() - start;
    }

    protected int putBoolean( String name , Boolean b ){
        int start = _buf.position();
        _put( BOOLEAN , name );
        _buf.put( b ? (byte)0x1 : (byte)0x0 );
        return _buf.position() - start;
    }

    protected int putDate( String name , Date d ){
        int start = _buf.position();
        _put( DATE , name );
        _buf.putLong( d.getTime() );
        return _buf.position() - start;
    }

    protected int putNumber( String name , Number n ){
        int start = _buf.position();
	if ( n instanceof Integer ){
	    _put( NUMBER_INT , name );
	    _buf.putInt( n.intValue() );
	}
	else {
	    _put( NUMBER , name );
	    _buf.putDouble( n.doubleValue() );
	}
        return _buf.position() - start;
    }

    protected void putBinary( String name , byte[] data ){
        
        _put( BINARY , name );
        _buf.putInt( 4 + data.length );

        _buf.put( B_BINARY );
        _buf.putInt( data.length );
        int before = _buf.position();
        _buf.put( data );
        int after = _buf.position();
        
        com.mongodb.util.MyAsserts.assertEquals( after - before , data.length );
    }

    protected int putSymbol( String name , DBSymbol s ){
        return _putString(name, s.getSymbol(), SYMBOL);
    }

    protected int putString(String name, String s) {
        return _putString(name, s, STRING);
    }

    private int _putString( String name , String s, byte type ){
        int start = _buf.position();
        _put( type , name );
        _putValueString( s );
        return _buf.position() - start;
    }

    protected int putObjectId( String name , ObjectId oid ){
        int start = _buf.position();
        _put( OID , name );
        _buf.putLong( oid._base );
        _buf.putInt( oid._inc );
        return _buf.position() - start;
    }
    
    protected int putDBRef( String name , String ns , ObjectId oid ){
        int start = _buf.position();
        _put( REF , name );
        
        _putValueString( ns );
        _buf.putLong( oid._base );
        _buf.putInt( oid._inc );

        return _buf.position() - start;
    }

    private int putDBRegex(String name, DBRegex regex) {

        int start = _buf.position();
        _put( REGEX , name );
        _put(regex.getPattern());

        String options = regex.getOptions();

        TreeMap<Character, Character> sm = new TreeMap<Character, Character>();

        for (int i=0; i < options.length(); i++) {
            sm.put(options.charAt(i), options.charAt(i));
        }

        StringBuffer sb = new StringBuffer();

        for (char c : sm.keySet()) {
            sb.append(c);
        }

        _put( sb.toString());
        return _buf.position() - start;

    }
    
    private int putPattern( String name, Pattern p ) {
        int start = _buf.position();
        _put( REGEX , name );
        _put( p.pattern() );
        _put( patternFlags( p.flags() ) );
        return _buf.position() - start;
    }


    // ----------------------------------------------
    
    private void _put( byte type , String name ){
        _buf.put( type );
        _put( name );
    }

    void _putValueString( String s ){
        int lenPos = _buf.position();
        _buf.putInt( 0 ); // making space for size
        int strLen = _put( s );
        _buf.putInt( lenPos , strLen );
    }
    
    int _put( String name ){

        _cbuf.position( 0 );
        _cbuf.limit( _cbuf.capacity() );
        _cbuf.append( name );
        
        _cbuf.flip();
        final int start = _buf.position();
        _encoder.encode( _cbuf , _buf , false );

        _buf.put( (byte)0 );

        return _buf.position() - start;
    }

    boolean _dontRefContains( Object o ){
        if ( _dontRef.size() == 0 )
            return false;
        return _dontRef.peek().contains( o );
    }
    
    private final CharBuffer _cbuf = CharBuffer.allocate( MAX_STRING );
    private final CharsetEncoder _encoder = _utf8.newEncoder();
    private Stack<IdentitySet> _dontRef = new Stack<IdentitySet>();
    
    private boolean _flipped = false;
    final ByteBuffer _buf;
}
