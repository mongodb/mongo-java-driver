package com.mongodb.codecs;

import org.bson.BSONObject;
import org.bson.BSONWriter;
import org.bson.types.CodeWScope;
import org.mongodb.Encoder;
import org.mongodb.codecs.Codecs;

public class CodeWScopeCodec implements Encoder<CodeWScope> {

    private final Codecs codecs;

    public CodeWScopeCodec(final Codecs codecs) {
        this.codecs = codecs;
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final CodeWScope value) {
        bsonWriter.writeJavaScriptWithScope(value.getCode());
        writeDocument(bsonWriter, value.getScope());
    }

    @Override
    public Class<CodeWScope> getEncoderClass() {
        return CodeWScope.class;
    }

    private void writeDocument(final BSONWriter bsonWriter, final BSONObject document) {
        bsonWriter.writeStartDocument();
        for (String key : document.keySet()){
            bsonWriter.writeName(key);
            codecs.encode(bsonWriter, document.get(key));
        }
        bsonWriter.writeEndDocument();
    }
}
