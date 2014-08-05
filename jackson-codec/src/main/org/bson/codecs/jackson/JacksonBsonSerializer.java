package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.codecs.jackson.JacksonBsonGenerator;

import java.io.IOException;

/**
 * Created by guo on 7/31/14.
 */
abstract class JacksonBsonSerializer<T> extends JsonSerializer<T> {
    @Override
    public void serialize(T t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        if (!(jsonGenerator instanceof JacksonBsonGenerator)) {
            throw new JsonGenerationException("JacksonBsonSerializer can " +
                    "only be used with JacksonBsonGenerator");
        }
        serialize(t, (JacksonBsonGenerator) jsonGenerator, serializerProvider);
    }

    public abstract void serialize(T t, JacksonBsonGenerator generator, SerializerProvider provider)
            throws IOException, JsonProcessingException;
}
