package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.BsonJavaScript;

import java.io.IOException;

/**
 * Created by guo on 8/1/14.
 */
class JacksonJavascriptSerializer extends JacksonBsonSerializer<BsonJavaScript> {

    @Override
    public void serialize(BsonJavaScript bsonJavaScript, JacksonBsonGenerator<BsonJavaScript> generator, SerializerProvider provider) throws IOException {
        if (bsonJavaScript == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeJavascript(bsonJavaScript);
        }
    }
}
