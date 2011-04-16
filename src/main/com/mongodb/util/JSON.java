// JSON.java

package com.mongodb.util;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import org.bson.BSONCallback;
import org.bson.types.*;

import com.mongodb.*;

/**
 *   Helper methods for JSON serialization and de-serialization
 */
public class JSON {

    /**
     *  Serializes an object into it's JSON form
     *
     * @param o object to serialize
     * @return  String containing JSON form of the object
     */
    public static String serialize( Object o ){
        StringBuilder buf = new StringBuilder();
        serialize( o , buf );
        return buf.toString();
    }

    static void string( StringBuilder a , String s ){
        a.append("\"");
        for(int i = 0; i < s.length(); ++i){
            char c = s.charAt(i);
            if (c == '\\')
                a.append("\\\\");
            else if(c == '"')
                a.append("\\\"");
            else if(c == '\n')
                a.append("\\n");
            else if(c == '\r')
                a.append("\\r");
            else if(c == '\t')
                a.append("\\t");
            else if(c == '\b')
                a.append("\\b");
            else if ( c < 32 )
                continue;
            else
                a.append(c);
        }
        a.append("\"");
    }

    @SuppressWarnings("unchecked")
    public static void serialize( Object o , StringBuilder buf ){
        
        o = Bytes.applyEncodingHooks( o );

        if ( o == null ){
            buf.append( " null " );
            return;
        }
        
        if ( o instanceof Number ){
            buf.append( o );
            return;
        }
        
        if ( o instanceof String ){
            string( buf , o.toString() );
            return;
        }

        if ( o instanceof Iterable){

            boolean first = true;
            buf.append( "[ " );
            
            for ( Object n : (Iterable)o ){
                if ( first ) first = false;
                else buf.append( " , " );
                
                serialize( n , buf );
            }
            
            buf.append( "]" );
            return;
        }


        if ( o instanceof ObjectId) {
	    serialize(new BasicDBObject("$oid", o.toString()), buf);
            return;
        }
        
        if ( o instanceof DBObject){
 
            boolean first = true;
            buf.append( "{ " );
            
            DBObject dbo = (DBObject)o;
            
            for ( String name : dbo.keySet() ){
                if ( first ) first = false;
                else buf.append( " , " );
                
                string( buf , name );
                buf.append( " : " );
                serialize( dbo.get( name ) , buf );
            }
            
            buf.append( "}" );
            return;
        }

        if ( o instanceof Map ){
 
            boolean first = true;
            buf.append( "{ " );
            
            Map m = (Map)o;

            for ( Map.Entry entry : (Set<Map.Entry>)m.entrySet() ){
                if ( first ) first = false;
                else buf.append( " , " );
                
                string( buf , entry.getKey().toString() );
                buf.append( " : " );
                serialize( entry.getValue() , buf );
            }
            
            buf.append( "}" );
            return;
        }


        if (o instanceof Date) {
            Date d = (Date) o;
            SimpleDateFormat format = 
		new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
	    serialize(new BasicDBObject("$date", format.format(d)), buf);
            return;
        }

        if (o instanceof DBRefBase) {
            DBRefBase ref = (DBRefBase)o;
            BasicDBObject temp = new BasicDBObject();
            temp.put( "$ref" , ref.getRef() );
            temp.put( "$id" , ref.getId() );
            serialize( temp, buf );
            return;
        }

        if (o instanceof Boolean) {
            buf.append(o);
            return;
	}

        if (o instanceof byte[] || o instanceof Binary) {
            buf.append("<Binary Data>");
            return;
        }

        if (o instanceof Pattern) {
	    DBObject externalForm = new BasicDBObject();
	    externalForm.put("$regex", o.toString());
	    externalForm.put("$options", Bytes.regexFlags( ((Pattern)o).flags() ) );
	    serialize(externalForm, buf);
            return;
        }

        if ( o.getClass().isArray() ){
            buf.append( "[ " );
            
            for ( int i=0; i<Array.getLength( o ); i++) {
                if ( i > 0 ) buf.append( " , " );
                serialize( Array.get( o , i ) , buf );
            }
            
            buf.append( "]" );
            return;
        }

        if ( o instanceof BSONTimestamp ){
            BSONTimestamp t = (BSONTimestamp)o;
            BasicDBObject temp = new BasicDBObject();
            temp.put( "$ts" , t.getTime() );
            temp.put( "$inc" , t.getInc() );
            serialize( temp, buf );
            return;
        }
        
        if ( o instanceof UUID ){
            UUID uuid = (UUID)o;
            BasicDBObject temp = new BasicDBObject();
            temp.put( "$uuid" , uuid.toString() );
            serialize( temp, buf );
            return;
        }

        if ( o instanceof CodeWScope ){
            CodeWScope c = (CodeWScope)o;
            
            BasicDBObject temp = new BasicDBObject();
            temp.put( "$code" , c.getCode() );
            temp.put( "$scope" , c.getScope() );
            serialize( temp, buf );
            return;
        }

        if ( o instanceof Code ){
            Code c = (Code)o;
            BasicDBObject temp = new BasicDBObject();
            temp.put( "$code" , c.getCode() );
            serialize( temp, buf );
            return;
        }
        
        throw new RuntimeException( "json can't serialize type : " + o.getClass() );
    }


    /**
     *  Parses a JSON string representing a JSON value
     *
     * @param s the string to parse
     * @return the object
     */
    public static Object parse( String s ){
	return parse( s, null );
    }

    /**
     * Parses a JSON string representing a JSON value
     *
     * @param s the string to parse
     * @return the object
     */
    public static Object parse( String s, BSONCallback c ){
        if (s == null || (s=s.trim()).equals("")) {
            return (DBObject)null;
        }

        JSONParser p = new JSONParser(s, c);
        return p.parse();
    }

}


/**
 * Parser for JSON objects.
 *
 * Supports all types described at www.json.org, except for
 * numbers with "e" or "E" in them.
 */
class JSONParser {

    String s;
    int pos = 0;
    BSONCallback _callback;

    /**
     * Create a new parser.
     */
    public JSONParser(String s) {
	this(s, null);
    }

    /**
     * Create a new parser.
     */
    public JSONParser(String s, BSONCallback callback) {
        this.s = s;
	_callback = (callback == null) ? new JSONCallback() : callback;
    }


    /**
     * Parse an unknown type.
     *
     * @return Object the next item
     * @throws JSONParseException if invalid JSON is found
     */
    public Object parse() {
	return parse(null);
    }

    /**
     * Parse an unknown type.
     *
     * @return Object the next item
     * @throws JSONParseException if invalid JSON is found
     */
    protected Object parse(String name) {
        Object value = null;
        char current = get();

        switch(current) {
        // null
        case 'n':
            read('n'); read('u'); read('l'); read('l');
	    value = null;
            break;
        // NaN
        case 'N':
            read('N'); read('a'); read('N');
	    value = Double.NaN;
            break;
        // true
        case 't':
            read('t'); read('r'); read('u'); read('e');
            value = true;
            break;
        // false
        case 'f':
            read('f'); read('a'); read('l'); read('s'); read('e');
            value = false;
            break;
        // string
        case '\'':
        case '\"':
            value = parseString();
            break;
        // number
        case '0': case '1': case '2': case '3': case '4': case '5':
        case '6': case '7': case '8': case '9': case '+': case '-':
            value = parseNumber();
            break;
        // array
        case '[':
            value = parseArray(name);
            break;
        // object
        case '{':
            value = parseObject(name);
            break;
        default:
            throw new JSONParseException(s, pos);
        }
        return value;
    }

    /**
     * Parses an object for the form <i>{}</i> and <i>{ members }</i>.
     *
     * @return DBObject the next object
     * @throws JSONParseException if invalid JSON is found
     */
    public Object parseObject() {
	return parseObject(null);
    }

    /**
     * Parses an object for the form <i>{}</i> and <i>{ members }</i>.
     *
     * @return DBObject the next object
     * @throws JSONParseException if invalid JSON is found
     */
    protected Object parseObject(String name){
	if (name != null) {
	    _callback.objectStart(name);
	} else {
	    _callback.objectStart();
	}

        read('{');
        char current = get();
        while(get() != '}') {
            String key = parseString();
            read(':');
            Object value = parse(key);
	    doCallback(key, value);

            if((current = get()) == ',') {
                read(',');
            }
            else {
                break;
            }
        }
        read('}');

        return _callback.objectDone();
    }
    
    protected void doCallback(String name, Object value) {
	if (value == null) {
	    _callback.gotNull(name);
	} else if (value instanceof String) {
	    _callback.gotString(name, (String)value);
	} else if (value instanceof Boolean) {
	    _callback.gotBoolean(name, (Boolean)value);
	} else if (value instanceof Integer) {
	    _callback.gotInt(name, (Integer)value);
	} else if (value instanceof Long) {
	    _callback.gotLong(name, (Long)value);
	} else if (value instanceof Double) {
	    _callback.gotDouble(name, (Double)value);
	} 
    }

    /**
     * Read the current character, making sure that it is the expected character.
     * Advances the pointer to the next character.
     *
     * @param ch the character expected
     *
     * @throws JSONParseException if the current character does not match the given character
     */
    public void read(char ch) {
        if(!check(ch)) {
            throw new JSONParseException(s, pos);
        }
        pos++;
    }

    public char read(){
        if ( pos >= s.length() )
            throw new IllegalStateException( "string done" );
        return s.charAt( pos++ );
    }

    /** 
     * Read the current character, making sure that it is a hexidecimal character.
     *
     * @throws JSONParseException if the current character is not a hexidecimal character
     */
    public void readHex() {
        if (pos < s.length() && 
            ((s.charAt(pos) >= '0' && s.charAt(pos) <= '9') ||
             (s.charAt(pos) >= 'A' && s.charAt(pos) <= 'F') ||
             (s.charAt(pos) >= 'a' && s.charAt(pos) <= 'f'))) {
            pos++;
        }
        else {
            throw new JSONParseException(s, pos);
        }
    }

    /**
     * Checks the current character, making sure that it is the expected character.
     *
     * @param ch the character expected
     *
     * @throws JSONParseException if the current character does not match the given character
     */
    public boolean check(char ch) {
        return get() == ch;
    }

    /**
     * Advances the position in the string past any whitespace.
     */
    public void skipWS() {
        while(pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
            pos++;
        }
    }

    /**
     * Returns the current character.
     * Returns -1 if there are no more characters.
     *
     * @return the next character
     */
    public char get() {
        skipWS();
        if(pos < s.length())
            return s.charAt(pos);
        return (char)-1;
    }

    /**
     * Parses a string.
     *
     * @return the next string.
     * @throws JSONParseException if invalid JSON is found
     */
    public String parseString() {
        char quot;
        if(check('\''))
            quot = '\'';
        else if(check('\"'))
            quot = '\"';
        else 
            throw new JSONParseException(s, pos);

        char current;

        read(quot);
        StringBuilder buf = new StringBuilder();
        int start = pos;
        while(pos < s.length() && 
              (current = s.charAt(pos)) != quot) {
            if(current == '\\') {
                pos++;
                
                char x = get();
                
                char special = 0;

                switch ( x ){

                case 'u':
                    { // decode unicode
                        buf.append(s.substring(start, pos-1));
                        pos++;
                        int tempPos = pos;
                        
                        readHex();
                        readHex();
                        readHex();
                        readHex();
                        
                        int codePoint = Integer.parseInt(s.substring(tempPos, tempPos+4), 16);
                        buf.append((char)codePoint);
                        
                        start = pos;
                        continue;
                    }
                case 'n': special = '\n'; break;
                case 'r': special = '\r'; break;
                case 't': special = '\t'; break;
                case 'b': special = '\b'; break;
                case '"': special = '\"'; break;
                case '\\': special = '\\'; break;
                }

                buf.append(s.substring(start, pos-1));
                if ( special != 0 ){
                    pos++;
                    buf.append( special );
                }
                start = pos;
                continue;
            }
            pos++;
        }
        read(quot);

        buf.append(s.substring(start, pos-1));
        return buf.toString();
    }

    /**
     * Parses a number.
     *
     * @return the next number (int or double).
     * @throws JSONParseException if invalid JSON is found
     */
    public Number parseNumber() {

        char current = get();
        int start = this.pos;
        boolean isDouble = false;

        if(check('-') || check('+')) {
            pos++;
        }

        outer:
        while(pos < s.length()) {
            switch(s.charAt(pos)) {
            case '0': case '1': case '2': case '3': case '4': 
            case '5': case '6': case '7': case '8': case '9':
                pos++;
                break;
            case '.':
                isDouble = true;
                parseFraction();
                break;
            case 'e': case 'E':
                isDouble = true;
                parseExponent();
                break;
            default:
                break outer;
            }
        }

        if (isDouble)
          return Double.valueOf(s.substring(start, pos));
        
        Long val = Long.valueOf(s.substring(start, pos));
        if (val <= Integer.MAX_VALUE)
            return val.intValue();
        return val;
    }

    /** 
     * Advances the pointed through <i>.digits</i>.
     */
    public void parseFraction() {
        // get past .
        pos++;

        outer:
        while(pos < s.length()) {
            switch(s.charAt(pos)) {
            case '0': case '1': case '2': case '3': case '4': 
            case '5': case '6': case '7': case '8': case '9':
                pos++;
                break;
            case 'e': case 'E':
                parseExponent();
                break;
            default:
                break outer;
            }
        }
    }

    /** 
     * Advances the pointer through the exponent.
     */
    public void parseExponent() {
        // get past E
        pos++;

        if(check('-') || check('+')) {
            pos++;
        }

        outer:
        while(pos < s.length()) {
            switch(s.charAt(pos)) {
            case '0': case '1': case '2': case '3': case '4': 
            case '5': case '6': case '7': case '8': case '9':
                pos++;
                break;
            default:
                break outer;
            }
        }
    }

    /**
     * Parses the next array.
     *
     * @return the array
     * @throws JSONParseException if invalid JSON is found
     */
    public Object parseArray() {
	return parseArray(null);
    }

    /**
     * Parses the next array.
     *
     * @return the array
     * @throws JSONParseException if invalid JSON is found
     */
    protected Object parseArray(String name) {
	if (name != null) {
	    _callback.arrayStart(name);
	} else {
	    _callback.arrayStart();
	}

        read('[');

	int i = 0;
        char current = get();
        while( current != ']' ) {
	    String elemName = String.valueOf(i++);
            Object elem = parse(elemName);
	    doCallback(elemName, elem);

            if((current = get()) == ',') {
                read(',');
            }
            else if(current == ']') {
                break;
            }
            else {
                throw new JSONParseException(s, pos);
            }
        }

        read(']');

        return _callback.arrayDone();
    }

}

/**
 * Exception throw when invalid JSON is passed to JSONParser.
 * 
 * This exception creates a message that points to the first 
 * offending character in the JSON string:
 * <pre>
 * { "x" : 3, "y" : 4, some invalid json.... }
 *                     ^
 * </pre>
 */
class JSONParseException extends RuntimeException { 

    private static final long serialVersionUID = -4415279469780082174L;

    String s;
    int pos;

    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(s);
        sb.append("\n");
        for(int i=0;i<pos;i++) {
            sb.append(" ");
        }
        sb.append("^");
        return sb.toString();
    }

    public JSONParseException(String s, int pos) {
        this.s = s;
        this.pos = pos;
    }
}
