package org.mongodb.codecs;

import org.bson.BSONWriter;
import org.mongodb.Encoder;

import java.lang.reflect.Field;

public class PojoEncoder implements Encoder<Object> {
    private final Codecs codecs;

    public PojoEncoder(final Codecs codecs) {
        this.codecs = codecs;
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final Object value) {
        bsonWriter.writeStartDocument();
        encodePojo(bsonWriter, value);
        bsonWriter.writeEndDocument();
    }

    private void encodePojo(final BSONWriter bsonWriter, final Object value) {
        for (Field field : value.getClass().getDeclaredFields()) {
            encodeField(bsonWriter, value, field);
        }
    }

    private void encodeField(final BSONWriter bsonWriter, final Object value, final Field field) {
        final String fieldName = field.getName();
        if (isValidFieldName(fieldName)) {
            try {
                field.setAccessible(true);
                final Object fieldValue = field.get(value);
                bsonWriter.writeName(fieldName);
                encodeValue(bsonWriter, fieldValue);
                field.setAccessible(false);
            } catch (IllegalAccessException e) {
                //TODO: this is really going to bugger up the writer if it throws an exception halfway through writing
                throw new EncodingException("Could not encode field '" + fieldName + "' from " + value, e);
            }
        }
    }

    private void encodeValue(final BSONWriter bsonWriter, final Object fieldValue) {
        if (codecs.canEncode(fieldValue)) {
            codecs.encode(bsonWriter, fieldValue);
        } else {
            encode(bsonWriter, fieldValue);
        }
    }

    private boolean isValidFieldName(final String fieldName) {
        //We need to document that fields starting with a $ will be ignored
        //and we probably need to be able to either disable this validation or make it pluggable
        return fieldName.matches("([a-zA-Z_][\\w$]*)");
    }

    @Override
    public Class<Object> getEncoderClass() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
