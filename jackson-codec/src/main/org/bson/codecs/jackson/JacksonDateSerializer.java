package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Date;

/**
 * Created by guo on 7/30/14.
 */
class JacksonDateSerializer extends JacksonBsonSerializer<Date> {
    @Override
    public void serialize(Date value, JacksonBsonGenerator<Date> generator, SerializerProvider provider) throws IOException {
        if (value == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeDate(value);
        }
    }
}
