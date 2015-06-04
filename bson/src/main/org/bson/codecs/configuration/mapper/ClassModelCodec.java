package org.bson.codecs.configuration.mapper;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

public class ClassModelCodec<T> implements Codec<T> {
    private final ClassModel classModel;

    public ClassModelCodec(final ClassModel model) {
        this.classModel = model;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        try {
            final Object entity = classModel.getType().newInstance();
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                final String name = reader.readName();
                final FieldModel fieldModel = classModel.getField(name);
                fieldModel.set(entity, fieldModel.getCodec().decode(reader, decoderContext));

            }
            reader.readEndDocument();
            return (T) entity;
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void encode(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        for (final FieldModel fieldModel : classModel.getFields()) {
            writer.writeName(fieldModel.getName());
            fieldModel.getCodec().encode(writer, fieldModel.get(value), encoderContext);

        }
        writer.writeEndDocument();
    }

    @Override
    public Class<T> getEncoderClass() {
        return classModel.getType();
    }
}
