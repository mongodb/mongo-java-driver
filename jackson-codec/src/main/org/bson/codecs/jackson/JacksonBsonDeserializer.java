package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.bson.codecs.jackson.JacksonBsonParser;

import java.io.IOException;

/**
 * Created by guo on 8/1/14.
 */
abstract class JacksonBsonDeserializer<T> extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser jsonParser, DeserializationContext ctxt)
            throws IOException {
        if (!(jsonParser instanceof JacksonBsonParser)) {
            throw new JsonGenerationException("BsonDeserializer can " +
                    "only be used with JacksonBsonParser");
        }
        return deserialize((JacksonBsonParser)jsonParser, ctxt);
    }

    public abstract T deserialize(JacksonBsonParser bp, DeserializationContext ctxt)
            throws IOException;
}
