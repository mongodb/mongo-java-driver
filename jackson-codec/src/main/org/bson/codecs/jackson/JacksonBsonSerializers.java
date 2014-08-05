package org.bson.codecs.jackson;

/**
 * Created by guo on 7/28/14.
 */

import com.fasterxml.jackson.databind.module.SimpleSerializers;
import org.bson.BsonJavaScript;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.regex.Pattern;


class JacksonBsonSerializers extends SimpleSerializers {
    private static final long serialVersionUID = -1327629614239143170L;

    /**
     * Default constructor
     */
    public JacksonBsonSerializers() {
        addSerializer(Date.class, new JacksonDateSerializer());
        addSerializer(Pattern.class, new JacksonRegexSerializer());
        addSerializer(ObjectId.class, new JacksonObjectIdSerializer());
        addSerializer(BsonSymbol.class, new JacksonSymbolSerializer());
        addSerializer(BsonTimestamp.class, new JacksonTimestampSerializer());
        addSerializer(BsonJavaScript.class, new JacksonJavascriptSerializer());

    }
}