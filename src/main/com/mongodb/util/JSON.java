// JSON.java

package com.mongodb.util;

import com.mongodb.ObjectId;
import com.mongodb.DBObject;

import java.util.List;

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

        throw new RuntimeException( "can't serialize type : " + o.getClass() );
    }
}
