package org.bson.codecs.jackson.deserializers;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import org.bson.BsonType;
import org.bson.codecs.jackson.JacksonBsonParser;

import java.io.IOException;
import java.util.Date;

/**
 * Created by guo on 7/30/14.
 */
public class JacksonDateDeserializer extends JacksonBsonDeserializer<Date> {

    @Override
    public Date deserialize(JacksonBsonParser parser, DeserializationContext ctxt)
            throws IOException {
        if (parser.getCurrentToken() != JsonToken.VALUE_EMBEDDED_OBJECT ||
                parser.getCurBsonType() != BsonType.DATE_TIME) {
            throw ctxt.mappingException(Date.class);
        }

        Object obj = parser.getEmbeddedObject();
        if (obj == null) {
            return null;
        }
        return (Date)obj;
    }
}
