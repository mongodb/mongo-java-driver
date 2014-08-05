package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by guo on 7/30/14.
 */
class JacksonRegexSerializer extends JacksonBsonSerializer<Pattern> {

    @Override
    public void serialize(Pattern value, JacksonBsonGenerator<Pattern> generator, SerializerProvider provider) throws IOException {
        if (value == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeRegex(value);
        }
    }
}
