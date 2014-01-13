/*
 * Copyright (c) 2008 MongoDB, Inc.
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
import org.bson.BSONWriter;
import org.bson.types.CodeWScope;
import org.mongodb.Encoder;
import org.mongodb.codecs.Codecs;

class CodeWScopeCodec implements Encoder<CodeWScope> {

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
        for (final String key : document.keySet()) {
            bsonWriter.writeName(key);
            codecs.encode(bsonWriter, document.get(key));
        }
        bsonWriter.writeEndDocument();
    }
}
