package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * Created by guo on 7/29/14.
 */
public class JacksonObjectIdSerializer extends JacksonBsonSerializer<ObjectId> {


    @Override
    public void serialize(ObjectId value, JacksonBsonGenerator<ObjectId> generator, SerializerProvider provider) throws IOException {
        if (value == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeObjectId(value);
        }
    }
}
