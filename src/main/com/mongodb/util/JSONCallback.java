// JSONCallback.java

package com.mongodb.util;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import org.bson.*;
import org.bson.types.*;

import com.mongodb.*;

public class JSONCallback extends BasicBSONCallback {
    
    @Override
    public BSONObject create(){
        return new BasicDBObject();
    }
    
    @Override
    protected BSONObject createList() {
        return new BasicDBList();
    }
    
    public void objectStart(boolean array, String name){
        _lastArray = array;
        super.objectStart( array , name );
    }

    public Object objectDone(){
        String name = curName();
        Object o = super.objectDone();
	BSONObject b = (BSONObject)o;

        // override the object if it's a special type
	if ( ! _lastArray ) {
	    if ( b.containsField( "$oid" ) ) {
		o = new ObjectId((String)b.get("$oid"));
		if (!isStackEmpty()) {
		    gotObjectId( name, (ObjectId)o);
		} else {
		    setRoot(o);
		}
	    } else if ( b.containsField( "$date" ) ) {
		SimpleDateFormat format = 
		    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
		o = format.parse((String)b.get("$date"), new ParsePosition(0));
		if (!isStackEmpty()) {
		    cur().put( name, o );
		} else {
		    setRoot(o);
		}
	    } else if ( b.containsField( "$regex" ) ) {
		o = Pattern.compile( (String)b.get( "$regex" ), 
				     BSON.regexFlags( (String)b.get( "$options" )) );
		if (!isStackEmpty()) {
		    cur().put( name, o );
		} else {
		    setRoot(o);
		}
	    } else if ( b.containsField( "$ts" ) ) {
                Long ts = ((Number)b.get("$ts")).longValue();
                Long inc = ((Number)b.get("$inc")).longValue();
		o = new BSONTimestamp(ts.intValue(), inc.intValue());
		if (!isStackEmpty()) {
		    cur().put( name, o );
		} else {
		    setRoot(o);
		}
	    } else if ( b.containsField( "$code" ) ) {
                if (b.containsField("$scope")) {
                    o = new CodeWScope((String)b.get("$code"), (DBObject)b.get("$scope"));
                } else {
                    o = new Code((String)b.get("$code"));
                }
		if (!isStackEmpty()) {
		    cur().put( name, o );
		} else {
		    setRoot(o);
		}
	    } else if ( b.containsField( "$ref" ) ) {
                o = new DBRef(null, (String)b.get("$ref"), b.get("$id"));
		if (!isStackEmpty()) {
		    cur().put( name, o );
		} else {
		    setRoot(o);
		}
	    } else if ( b.containsField( "$uuid" ) ) {
                o = UUID.fromString((String)b.get("$uuid"));
		if (!isStackEmpty()) {
		    cur().put( name, o );
		} else {
		    setRoot(o);
		}
	    }
	}
        return o;
    }

    private boolean _lastArray = false;
}
