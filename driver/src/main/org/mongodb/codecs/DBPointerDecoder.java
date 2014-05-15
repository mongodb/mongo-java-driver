/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.types.DBPointer;
import org.mongodb.DBRef;
import org.bson.codecs.Decoder;

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
