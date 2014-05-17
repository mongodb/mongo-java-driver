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

package org.bson.codecs;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.DBPointer;

/**
 * Converts BSON type DBPointer(0x0c) to database references as DBPointer is deprecated.
 *
 * @since 3.0
 */
public class DBPointerCodec implements Codec<DBPointer> {

    @Override
    public DBPointer decode(final BSONReader reader) {
        return reader.readDBPointer();
    }

    @Override
    public void encode(final BSONWriter writer, final DBPointer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<DBPointer> getEncoderClass() {
        return DBPointer.class;
    }
}
