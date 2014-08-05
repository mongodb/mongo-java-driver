package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.codecs.jackson.JacksonBsonGenerator;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by guo on 7/30/14.
 */
class JacksonRegexSerializer extends JacksonBsonSerializer<Pattern> {

    @Override
    public void serialize(Pattern value, JacksonBsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
        if (value == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeRegex(value);
        }
    }
}
