package com.mongodb.codecs;

import org.bson.BSONBinarySubType;
import org.bson.BSONReader;
import org.bson.types.Binary;
import org.mongodb.Decoder;
import org.mongodb.codecs.UUIDCodec;

public class LegacyBinaryDecoder implements Decoder<Object> {

    @Override
    public Object decode(final BSONReader reader) {
        Binary binary = reader.readBinaryData();
        if (binary.getType() == BSONBinarySubType.Binary.getValue()) {
            return binary.getData();
        } else if (binary.getType() == BSONBinarySubType.UuidLegacy.getValue()) {
            return new UUIDCodec().toUUID(binary.getData());
        } else {
            return binary;
        }
    }
}
