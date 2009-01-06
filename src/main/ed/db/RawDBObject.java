// RawDBObject.java

package ed.db;

import java.nio.*;
import java.util.*;

import static ed.db.Bytes.*;
import static ed.MyAsserts.*;

/* This object wraps the binary object format ("BSON") used for the transport of serialized objects 
   to / from the Mongo database.

   http://www.10gen.com/wiki/bson
*/
public class RawDBObject implements DBObject {

    static boolean DEBUG = Boolean.getBoolean( "DEBUG.DBJSO" );

    RawDBObject( ByteBuffer buf ){
        this( buf , 0 );
        assertEquals( _end , _buf.limit() );
    }
    
    RawDBObject( ByteBuffer buf , int offset ){
        _buf = buf;
        _offset = offset;
        _end = _buf.getInt( _offset );
    }

    public Object get( String key ){
        Element e = findElement( key );
        if ( e == null )
            return null;
        return e.getObject();
    }


    public Object put( String key , Object v ){
        throw new RuntimeException( "read only" );
    }

    public Object removeField( String key ){
        throw new RuntimeException( "read only" );
    }

    public boolean containsKey( String key ){
        return findElement( key ) != null;
    }

    public Set<String> keySet(){    
        Set<String> keys = new HashSet<String>();
        
        ElementIter i = new ElementIter();
        while ( i.hasNext() ){
            Element e = i.next();
	    if ( e.eoo() )
		break;
            keys.add( e.fieldName() );
        }
        
        return keys;
    }
    String _readCStr( final int start ){
	return _readCStr( start , null );
    }
    
    String _readCStr( final int start , final int[] end ){
        synchronized ( _cStrBuf ){
            int pos = 0;
            while ( _buf.get( pos + start ) > 0 ){
                _cStrBuf[pos] = _buf.get( pos + start );
                pos++;
            }
            if ( end != null && end.length > 0 )
                end[0] = start + pos;
            return new String( _cStrBuf , 0 , pos );

        }
    }

    String _readJavaString( final int start ){
	int size = _buf.getInt( start ) - 1;
	
	byte[] b = new byte[size];

	int old = _buf.position();
	_buf.position( start + 4 );
	_buf.get( b , 0 , b.length );
	_buf.position( old );
	
	try {
	    return new String( b , "UTF-8" );
	}
	catch ( java.io.UnsupportedEncodingException uee ){
	    return new String( b );
	}
    }

    /**
     * includes 0 at end
     */
    int _cStrLength( final int start ){
	int end = start;
	while ( _buf.get( end ) > 0 )
	    end++;
	return 1 + ( end - start );
    }

    Element findElement( String name ){
        ElementIter i = new ElementIter();
        while ( i.hasNext() ){
            Element e = i.next();
            if ( e.fieldName().equals( name ) )
                return e;
        }
        return null;
    }

    public boolean isPartialObject(){
        return false;
    }

    public String toString(){
        return "Object";
    }
    
    class Element {
        Element( final int start ){
            _start = start;
            _type = _buf.get( _start );
            _name = eoo() ? "" : _readCStr( _start + 1 );
            
	    if ( DEBUG ) System.out.println( "name [" + _name + "] type [" + _type + "]" );
	    
            int size = 1 + _name.length() + 1; // 1 for the start byte, end for end of string
            _dataStart = _start + size;

            switch ( _type ){
            case MAXKEY:
            case EOO:
            case UNDEFINED:
            case NULL:
                break;
            case BOOLEAN:
                size += 1;
                break;
            case DATE:
            case NUMBER:
                size += 8;
                break;
	    case NUMBER_INT:
		size += 4;
		break;
            case OID:
                size += 12;
                break;
            case REF:
                size += 12;
            case SYMBOL:
            case CODE:
            case STRING:
                size += 4 + _buf.getInt( _dataStart );
                break;
            case CODE_W_SCOPE:
            case ARRAY:
            case OBJECT:
		size += _buf.getInt( _dataStart );
		break;
            case BINARY:
                size += 4 + _buf.getInt( _dataStart ) + 1;
                break;
            case REGEX:
		int first = _cStrLength( _dataStart );
		int second = _cStrLength( _dataStart + first );
		size += first + second;
		break;
            default:
                throw new RuntimeException( "can't handle type " + _type );
            }
            _size = size;
        }

        String fieldName(){
            return _name;
        }

        boolean eoo(){
            return _type == EOO || _type == MAXKEY;
        }
	
        int size(){
            return _size;
        }
	
        Object getObject(){
            
            if ( _cached != null )
                return _cached;
            
            switch ( _type ){
            case NUMBER:
                return _buf.getDouble( _dataStart );
	    case NUMBER_INT:
		return _buf.getInt( _dataStart );
	    case OID:
		return new ObjectId( _buf.getLong( _dataStart ) , _buf.getInt( _dataStart + 8 ) );
	    case CODE:
            case CODE_W_SCOPE:
                throw new RuntimeException( "can't handle code" );
	    case SYMBOL:
	    case STRING:
		return _readJavaString( _dataStart );
	    case DATE:
		return new Date( _buf.getLong( _dataStart ) );
	    case REGEX:
		//int[] endPos = new int[1];
		//String first = _readCStr( _dataStart , endPos );
		//return new JSRegex( first , _readCStr( 1 + endPos[0] ) );
                throw new RuntimeException( "can't handle regex" );
	    case BINARY:
		throw new RuntimeException( "can't inspect binary in db" );
	    case BOOLEAN:
		return _buf.get( _dataStart ) > 0;
	    case ARRAY:
	    case OBJECT:
                throw new RuntimeException( "can't handle emebdded objects" );
	    case NULL:
            case EOO:
	    case MAXKEY:
	    case UNDEFINED:
                return null;
            }
            throw new RuntimeException( "can't decode type " + _type );
        }

        final int _start;
        final byte _type;
        final String _name;
        final int _dataStart;
        final int _size;

        Object _cached;
    }
    
    class ElementIter {
        
        ElementIter(){
            _pos = _offset + 4;
        }
        
        boolean hasNext(){
            return ! _done && _pos < _buf.limit();
        }
        
        Element next(){
            Element e = new Element( _pos );
            _done = e.eoo();
                
            _pos += e.size();
            return e;
        }
        
        int _pos;
        boolean _done = false;
    }

    final ByteBuffer _buf;
    final int _offset;
    final int _end;
    private final static byte[] _cStrBuf = new byte[1024];
}
