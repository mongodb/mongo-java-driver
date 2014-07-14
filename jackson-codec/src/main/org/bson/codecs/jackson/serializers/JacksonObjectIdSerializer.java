package org.bson.codecs.jackson.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.codecs.jackson.JacksonBsonGenerator;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * Created by guo on 7/29/14.
 */
public class JacksonObjectIdSerializer extends JacksonBsonSerializer<ObjectId> {


    @Override
    public void serialize(ObjectId value, JacksonBsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
        if (value == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeObjectId(value);
        }
    }
}
