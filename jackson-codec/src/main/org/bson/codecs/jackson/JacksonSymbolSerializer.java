package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.BsonSymbol;

import java.io.IOException;

/**
 * Created by guo on 7/30/14.
 */
class JacksonSymbolSerializer extends JacksonBsonSerializer<BsonSymbol> {

    @Override
    public void serialize(BsonSymbol value, JacksonBsonGenerator<BsonSymbol> generator, SerializerProvider provider) throws IOException {
        if (value == null) {
            provider.defaultSerializeNull(generator);
        } else {
            generator.writeSymbol(value);
        }
    }
}

