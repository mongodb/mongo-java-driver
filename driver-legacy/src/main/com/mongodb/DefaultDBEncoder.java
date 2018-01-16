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

package com.mongodb;

import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.io.OutputBuffer;

/**
 * The default BSON encoder for BSONObject instances.
 */
public class DefaultDBEncoder extends BasicBSONEncoder implements DBEncoder {

    @Override
    public int writeObject(final OutputBuffer outputBuffer, final BSONObject document) {
        set(outputBuffer);
        int x = putObject(document);
        done();
        return x;
    }

    @Override
    protected boolean putSpecial(final String name, final Object value) {
        if (value instanceof DBRef) {
            putDBRef(name, (DBRef) value);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Deals with encoding database references.
     *
     * @param name the name of the field in the document
     * @param ref  the database reference object
     */
    protected void putDBRef(final String name, final DBRef ref) {
        BasicDBObject dbRefDocument = new BasicDBObject("$ref", ref.getCollectionName()).append("$id", ref.getId());
        if (ref.getDatabaseName() != null) {
            dbRefDocument.put("$db", ref.getDatabaseName());
        }

        putObject(name, dbRefDocument);
    }

    @Override
    public String toString() {
        return String.format("DBEncoder{class=%s}", getClass().getName());
    }

    public static final DBEncoderFactory FACTORY = new DBEncoderFactory() {
        @Override
        public DBEncoder create() {
            return new DefaultDBEncoder();
        }
    };
}
