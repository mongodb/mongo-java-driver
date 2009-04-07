// JSON.java

package com.mongodb.util;

import com.mongodb.ObjectId;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.SimpleTimeZone;

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
            if ( c < 32 )
                continue;
            if(c == '\\')
                a.append("\\\\");
            else if(c == '"')
                a.append("\\\"");
            else if(c == '\n')
                a.append("\\n");
            else if(c == '\r')
                a.append("\\r");
            else if(c == '\t')
                a.append("\\t");
            else
                a.append(c);
        }
        a.append("\"");
    }
    public static void serialize( Object o , StringBuilder buf ){
        
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

        if ( o instanceof List){

            boolean first = true;
            buf.append( "[ " );
            
            for ( Object n : (List)o ){
                if ( first ) first = false;
                else buf.append( " , " );
                
                serialize( n , buf );
            }
            
            buf.append( "]" );
            return;
        }

        if ( o instanceof ObjectId) {
            string(buf, o.toString());
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

        if (o instanceof Date) {
            Date d = (Date) o;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
            buf.append('"').append(format.format(d)).append('"');
            return;
        }

        if (o instanceof Boolean) {
            buf.append(o);
            return;
        }

        throw new RuntimeException( "can't serialize type : " + o.getClass() );
    }


    /**
     *  Parses a JSON string into a DBObject.
     *
     * @param s the string to serialize
     * @return DBObject the object
     */
    public static DBObject parse( String s ){
        if (s == null || (s=s.trim()).equals("")) {
            return (DBObject)null;
        }

        JSONParser p = new JSONParser(s);
        return p.parseObject();
    }

}


/**
 * Parser for JSON objects.
 */
class JSONParser {

    String s;
    int pos = 0;

    /**
     * Create a new parser.
     */
    public JSONParser(String s) {
        this.s = s;
    }


    /**
     * Parse an unknown type.
     */
    public Object parse() {
        Object value = null;
        char current = get();

        switch(current) {
            // null
        case 'n':
            read('n'); read('u'); read('l'); read('l');
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
            value = parseArray();
            break;
            // object
        case '{':
            value = parseObject();
            break;
        }
        return value;
    }

    /**
     * Parses an object for the form 
     */
    public DBObject parseObject() {
        DBObject obj = new BasicDBObject();
        read('{');

        char current = get();
        while(current != '}') {
            String key = parseString();
            read(':');
            Object value = parse();

            obj.put(key, value);

            if((current = get()) == ',') {
                read(',');
            }
            else {
                read('}');
                break;
            }
        }

        return obj;
    }

    public void read(char ch) {
        if(!check(ch)) {
            throw new JSONParseException(s, pos);
        }
        pos++;
    }

    public boolean check(char ch) {
        return get() == ch;
    }

    public void skipWS() {
        while(pos < s.length() && s.charAt(pos) == ' ') {
            pos++;
        }
    }

    public char get() {
        skipWS();
        if(pos < s.length())
            return s.charAt(pos);
        return (char)-1;
    }

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
        int start = pos;
        while((current = s.charAt(pos)) != quot) {
            if(current == '\\')
                s.charAt(pos++);
            s.charAt(pos++);
        }
        read(quot);
        return s.substring(start, pos-1);
    }

    /**
     * Parses a number (only an int, for now)
     */
    public double parseNumber() {

        char current = get();
        int start = this.pos;

        if(check('-') || check('+')) {
            pos++;
        }

        outer:
        while(true) {
            switch(s.charAt(pos)) {
            case '0': case '1': case '2': case '3': case '4': 
            case '5': case '6': case '7': case '8': case '9':
                pos++;
                break;
            case '.':
                parseFraction();
                break;
            default:
                break outer;
            }
        }

        return Double.parseDouble(s.substring(start, pos));
    }

    /** 
     * Parses <i>.digits</i>
     */
    public void parseFraction() {
        // get past .
        pos++;

        outer:
        while(true) {
            switch(s.charAt(pos)) {
            case '0': case '1': case '2': case '3': case '4': 
            case '5': case '6': case '7': case '8': case '9':
                pos++;
                break;
            case 'e':
            case 'E':
                parseExponent();
                break;
            default:
                break outer;
            }
        }
    }


    /** 
     * Parses <i>e digits</i>
     */
    public void parseExponent() {
        // get past E
        pos++;

        if(check('-') || check('+')) {
            pos++;
        }

        outer:
        while(true) {
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


    public DBObject parseArray() {
        read('[');
        DBObject obj = new BasicDBObject();

        int count = 0;
        char current = get();
        while( current != ']' ) {
            String key = "" + count;
            Object value = parse();
            obj.put(key, value);

            count++;
        }

        read(']');
        return obj;
    }

}


class JSONParseException extends RuntimeException { 

    String s;
    int pos;

    public String getMessage() {
        StringBuffer sb = new StringBuffer();
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