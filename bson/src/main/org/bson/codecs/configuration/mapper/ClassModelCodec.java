package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.TypeResolver;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.util.HashMap;
import java.util.Map;

public class ClassModelCodec<T> implements Codec<T> {
    private final ClassModel classModel;

    public ClassModelCodec(final ClassModel model) {
        this.classModel = model;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return classModel.decode(reader, decoderContext);
    }

    @Override
    public void encode(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        classModel.encode(writer, value, encoderContext);
    }

    @Override
    public Class<T> getEncoderClass() {
        return classModel.getType();
    }
}
