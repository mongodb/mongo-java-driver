// JSONCallback.java

package com.mongodb.util;

import java.text.*;
import java.util.*;
import java.util.logging.*;
import java.util.regex.*;

import org.bson.*;
import org.bson.types.*;
import com.mongodb.*;

public class JSONCallback extends BasicBSONCallback {
    
    public BSONObject create(){
        return new BasicDBObject();
    }
    
    public BSONObject create( boolean array , List<String> path ){
        if ( array )
            return new BasicDBList();
        return new BasicDBObject();
    }

    public void objectStart(boolean array, String name){
        _lastName = name;
        _lastArray = array;
        super.objectStart( array , name );
    }

    public Object objectDone(){
        Object o = super.objectDone();
	BSONObject b = (BSONObject)o;

	if ( ! _lastArray ) {
	  if ( b.containsKey( "$oid" ) ) {
		  o = new ObjectId((String)b.get("$oid"));
		  if (!isStackEmpty()) {
		    gotObjectId( _lastName, (ObjectId)o);
  		} else {
		    setRoot(o);
	  	}
	  } else if ( b.containsKey( "$date" ) ) {
		  SimpleDateFormat format = new SimpleDateFormat(JSON.ISO_8601_DATE_FORMAT);
      String date = (String) b.get("$date");
      // for backward compatibility 
      if(date.contains("Z")) {
        date = date.substring(0, date.indexOf('Z')) + "+0000";
      }
      o = format.parse(date, new ParsePosition(0));
		  if (!isStackEmpty()) {
		    cur().put( _lastName, o );
		  } else {
		    setRoot(o);
  		}
	  } else if ( b.containsKey( "$regex" ) ) {
	  	o = Pattern.compile( (String)b.get( "$regex" ),
			    BSON.regexFlags( (String)b.get( "$options" )) );
		  if (!isStackEmpty()) {
		    cur().put( _lastName, o );
		  } else {
		    setRoot(o);
		  }
	  }
	}

        return o;
    }

    private String _lastName;
    private boolean _lastArray = false;
}
