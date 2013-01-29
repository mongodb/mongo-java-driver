// JSONCallback.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.types.*;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.UUID;
import java.util.regex.Pattern;

public class JSONCallback extends BasicBSONCallback {

    @Override
    public BSONObject create() {
        return new BasicDBObject();
    }

    @Override
    protected BSONObject createList() {
        return new BasicDBList();
    }

    public void objectStart(boolean array, String name) {
        _lastArray = array;
        super.objectStart(array, name);
    }

    public Object objectDone() {
        String name = curName();
        Object o = super.objectDone();
        if (_lastArray) {
            return o;
        }
        BSONObject b = (BSONObject) o;

        // override the object if it's a special type
        if (b.containsField("$oid")) {
            o = new ObjectId((String) b.get("$oid"));
        } else if (b.containsField("$date")) {
            if (b.get("$date") instanceof Number) {
                o = new Date(((Number) b.get("$date")).longValue());
            } else {
                SimpleDateFormat format = new SimpleDateFormat(_msDateFormat);
                format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
                o = format.parse(b.get("$date").toString(), new ParsePosition(0));

                if (o == null) {
                    // try older format with no ms
                    format = new SimpleDateFormat(_secDateFormat);
                    format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
                    o = format.parse(b.get("$date").toString(), new ParsePosition(0));
                }
            }
        } else if (b.containsField("$regex")) {
            o = Pattern.compile((String) b.get("$regex"),
                    BSON.regexFlags((String) b.get("$options")));
        } else if (b.containsField("$ts")) { //Legacy timestamp format
            Integer ts = ((Number) b.get("$ts")).intValue();
            Integer inc = ((Number) b.get("$inc")).intValue();
            o = new BSONTimestamp(ts, inc);
        } else if (b.containsField("$timestamp")) {
            BSONObject tsObject = (BSONObject) b.get("$timestamp");
            Integer ts = ((Number) tsObject.get("t")).intValue();
            Integer inc = ((Number) tsObject.get("i")).intValue();
            o = new BSONTimestamp(ts, inc);
        } else if (b.containsField("$code")) {
            if (b.containsField("$scope")) {
                o = new CodeWScope((String) b.get("$code"), (DBObject) b.get("$scope"));
            } else {
                o = new Code((String) b.get("$code"));
            }
        } else if (b.containsField("$ref")) {
            o = new DBRef(null, (String) b.get("$ref"), b.get("$id"));
        } else if (b.containsField("$minKey")) {
            o = new MinKey();
        } else if (b.containsField("$maxKey")) {
            o = new MaxKey();
        } else if (b.containsField("$uuid")) {
            o = UUID.fromString((String) b.get("$uuid"));
        } else if (b.containsField("$binary")) {
            int type = (Integer) b.get("$type");
            byte[] bytes = (new Base64Codec()).decode((String) b.get("$binary"));
            o = new Binary((byte) type, bytes);
        }

        if (!isStackEmpty()) {
            _put(name, o);
        } else {
            o = !BSON.hasDecodeHooks() ? o : BSON.applyDecodingHooks( o );
            setRoot(o);
        }
        return o;
    }

    private boolean _lastArray = false;

    public static final String _msDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String _secDateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'";
}
