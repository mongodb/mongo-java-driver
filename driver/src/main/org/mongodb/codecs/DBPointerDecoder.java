package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.types.DBPointer;
import org.mongodb.DBRef;
import org.mongodb.Decoder;

/**
 * Converts BSON type DBPointer(0x0c) to database references as DBPointer is deprecated.
 */
public class DBPointerDecoder implements Decoder<DBRef> {

    @Override
    public DBRef decode(final BSONReader reader) {
        DBPointer dbPointer = reader.readDBPointer();
        return new DBRef(dbPointer.getId(), dbPointer.getNamespace());
    }

}
