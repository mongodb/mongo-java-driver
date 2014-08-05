package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.BsonTimestamp;

import java.io.IOException;

/**
 * Created by guo on 8/1/14.
 */
class JacksonTimestampSerializer extends JacksonBsonSerializer<BsonTimestamp> {
    @Override
    public void serialize(BsonTimestamp timestamp, JacksonBsonGenerator<BsonTimestamp> bsonGenerator, SerializerProvider provider) throws IOException {
        if (timestamp == null) {
            provider.defaultSerializeNull(bsonGenerator);
        } else {
            bsonGenerator.writeTimestamp(timestamp);
        }
    }
}
