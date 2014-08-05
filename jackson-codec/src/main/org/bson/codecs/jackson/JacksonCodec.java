package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.BsonDocumentReader;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.io.IOException;

/**
 * A codec for BsonArray instances.
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
            T decodedObject = objectMapper.readValue(p,clazz);
            return decodedObject;

        } catch (IOException e) {

        };

        return null;
    }

    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        JacksonBsonGenerator<T> generator = new JacksonBsonGenerator<T>(writer);

        try {

            ObjectWriter w = objectMapper.writerWithType(clazz);
            w.writeValue(generator, value);
            generator.close();

        } catch (JsonGenerationException e) {
            System.out.println(e);
        } catch (IOException e) {
            System.out.println(e);
        }
    }
    public Class<T> getEncoderClass() {
        return clazz;
    }

}
