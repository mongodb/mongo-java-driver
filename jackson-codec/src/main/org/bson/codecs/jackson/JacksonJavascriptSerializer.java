package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.BsonJavaScript;
import org.bson.codecs.jackson.JacksonBsonGenerator;

import java.io.IOException;

/**
 * Created by guo on 8/1/14.
 */
class JacksonJavascriptSerializer extends JacksonBsonSerializer<BsonJavaScript> {

    @Override
    public void serialize(BsonJavaScript bsonJavaScript, JacksonBsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
        if (bsonJavaScript == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeJavascript(bsonJavaScript);
        }
    }
}
