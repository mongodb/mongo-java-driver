package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.BsonSymbol;
import org.bson.codecs.jackson.JacksonBsonGenerator;
import org.bson.types.Symbol;

import java.io.IOException;

/**
 * Created by guo on 7/30/14.
 */
class JacksonSymbolSerializer extends JacksonBsonSerializer<BsonSymbol> {

    @Override
    public void serialize(BsonSymbol value, JacksonBsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
        if (value == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeSymbol(value);
        }
    }
}

