package com.mongodb.codecs;

import com.mongodb.DBRef;
import org.bson.BSONWriter;
import org.mongodb.Encoder;
import org.mongodb.codecs.Codecs;

public class DBRefEncoder implements Encoder<DBRef> {

    private final Codecs codecs;

    public DBRefEncoder(Codecs codecs) {
        this.codecs = codecs;
    }

    @Override
    public void encode(BSONWriter bsonWriter, DBRef value) {
        bsonWriter.writeStartDocument();

        bsonWriter.writeString("$ref", value.getRef());
        bsonWriter.writeName("$id");
        codecs.encode(bsonWriter, value.getId());

        bsonWriter.writeEndDocument();
    }

    @Override
    public Class<DBRef> getEncoderClass() {
        return DBRef.class;
    }
}
