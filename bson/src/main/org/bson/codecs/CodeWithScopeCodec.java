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
import org.bson.types.BsonDocument;
import org.bson.types.CodeWithScope;

/**
 * A Codec for CodeWithScope instances.
 *
 * @since 3.0
 */
public class CodeWithScopeCodec implements Codec<CodeWithScope> {
    private final Codec<BsonDocument> documentCodec;

    /**
     * Construct a new instance with the given codec to use for the nested document
     *
     * @param documentCodec the non-null codec for the nested document
     */
    public CodeWithScopeCodec(final Codec<BsonDocument> documentCodec) {
        this.documentCodec = documentCodec;
    }

    @Override
    public CodeWithScope decode(final BSONReader bsonReader) {
        String code = bsonReader.readJavaScriptWithScope();
        BsonDocument scope = documentCodec.decode(bsonReader);
        return new CodeWithScope(code, scope);
    }

    @Override
    public void encode(final BSONWriter writer, final CodeWithScope codeWithScope) {
        writer.writeJavaScriptWithScope(codeWithScope.getCode());
        documentCodec.encode(writer, codeWithScope.getScope());
    }

    @Override
    public Class<CodeWithScope> getEncoderClass() {
        return CodeWithScope.class;
    }
}
