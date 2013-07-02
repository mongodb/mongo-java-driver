package org.mongodb.command;

import org.bson.BSONReader;
import org.bson.BSONType;
import org.mongodb.Decoder;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;

import java.util.ArrayList;
import java.util.List;

public class MapReduceCommandResultCodec<T> extends DocumentCodec {

    private final Decoder<T> decoder;

    public MapReduceCommandResultCodec(final PrimitiveCodecs primitiveCodecs, final Decoder<T> decoder) {
        super(primitiveCodecs);
        this.decoder = decoder;
    }

    @Override
    protected Object readValue(final BSONReader reader, final String fieldName) {
        if ("results".equals(fieldName)) {
            return readArray(reader);
        } else {
            return super.readValue(reader, fieldName);
        }
    }

    private List<T> readArray(final BSONReader reader) {
        final List<T> list = new ArrayList<T>();
        reader.readStartArray();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            list.add(decoder.decode(reader));
        }
        reader.readEndArray();
        return list;
    }
}
