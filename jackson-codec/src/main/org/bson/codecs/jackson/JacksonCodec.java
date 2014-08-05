package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.BsonReader;
import org.bson.BsonSerializationException;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.io.IOException;

/**
 * A codec for  instances.
 *
 * @since 3.0
 */
public class JacksonCodec<T> implements Codec<T> {

    private final Class<T> clazz;

    private final ObjectMapper objectMapper;

    public JacksonCodec(Class<T> clazz) {
        this(new ObjectMapper(), clazz);
    }

    public JacksonCodec(ObjectMapper objMapper, Class<T> clazz) {

        objectMapper = objMapper;
        objectMapper.registerModule(new JacksonBsonModule());
        this.clazz = clazz;
    }


    public T decode(final BsonReader reader, final DecoderContext decoderContext) {

        try {
            JacksonBsonParser p = new JacksonBsonParser(reader);
            return objectMapper.readValue(p,clazz);

        } catch (IOException e) {
            throw new BsonSerializationException("error reading value", e);
        }

    }

    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        JacksonBsonGenerator<T> generator = new JacksonBsonGenerator<T>(writer);

        try {

            ObjectWriter w = objectMapper.writerWithType(clazz);
            w.writeValue(generator, value);
            generator.close();

        } catch (IOException e) {
            throw new BsonSerializationException("error writing value " + value, e);
        }
    }
    public Class<T> getEncoderClass() {
        return clazz;
    }

}
